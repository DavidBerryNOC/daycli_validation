import pandas as pd

class bufr_message:
    idx = 0
    current_subset = pd.DataFrame( {'ElementName':[],'FXY':[],'Value':[],'Units':[]} )
    def __init__(self, table_B, table_D):
        self.table_B = pd.read_csv(table_B,sep=',', dtype='object')
        self.table_B['BUFR_DataWidth_Bits'] = self.table_B['BUFR_DataWidth_Bits'].map(int)
        self.table_B['BUFR_Scale'] = self.table_B['BUFR_Scale'].map(int)
        self.table_B['BUFR_ReferenceValue'] = self.table_B['BUFR_ReferenceValue'].map(float)
        self.table_D = pd.read_csv(table_D, sep=',', dtype='object')

        self.table_B_tmp = self.table_B.copy()

    def expand_sequence(self, sequence ):
        # first iterate over sequence performing any replication
        idx = 0
        content = list()
        sequence_length = len( sequence )
        while idx < sequence_length :
            d = sequence[idx]
            idx += 1
            if d[0] == '1':
                nelem = int(d[1:3]) # get number of elements
                nreps = int(d[3:6]) # number of replications
                assert nelem > 0, "Replication error, nelem < 1"
                assert nreps > 0, "Delayed replication unsupported in call to expand_sequence"
                for rep in range( nreps ) :
                    content.extend( sequence[idx : idx + nelem] )
                idx += nelem
            else :
                content.append( d )
        # next expand D sequences
        content2 = list()
        nested = False
        sequence_length = len( content )
        idx = 0
        while idx < sequence_length :
            d = content[ idx ]
            idx += 1
            if d[0] == '3' :
                expanded_sequence = (list(self.table_D.loc[self.table_D['FXY1'] == d, 'FXY2'].copy()))
                for e in expanded_sequence:
                    if (e[0] == '3') | (e[0] == '1'):
                        nested = True
                    content2.append( e )
            else:
                content2.append( d )
        if nested:
            content2 = self.expand_sequence( content2 )
        return( content2 )

    def old_expand_sequence(self, sequence):
        content = list()
        idx = 0
        sequence_length = len( sequence )
        #print(sequence)
        while idx < sequence_length :
        #for d in sequence:
            d = sequence[ idx ]
            if d[0] == '3':
                expanded_sequence = (list(self.table_D.loc[self.table_D['FXY1'] == d, 'FXY2'].copy()))
                idx += 1
                for e in expanded_sequence:
                    content.append( self.expand_sequence( [e] ) )
            elif d[0] == '1':
                #print('Replication')
                assert int( d[1:3] ) > 0
                assert int( d[3:6] ) > 0
                nelem = int( d[1:3] )
                nreps = int( d[3:6] )
                idx += 1
                for rep in range( nreps ):
                    content.append( sequence[ idx : idx + nelem] )
                idx += nelem
            else:
                content.append(d)
                idx += 1
        return (content)


    def read_compressed_sequence(self , sequence, bits, nsubsets, reset = True ) :
        assert isinstance(sequence, list)
        if reset:
            self.table_B_tmp = self.table_B.copy()
        sequence_length = len( sequence )
        df_index = pd.MultiIndex( levels = [[],[]], codes = [[],[]], names = ['subset','element_index'])
        subsets = pd.DataFrame(index = df_index, columns = {'element_name': [], 'FXY': [], 'value': [],
                                                            'units': []})#, 'width':[], 'scale':[], 'reference':[]})
        sidx = 0
        while sidx < sequence_length:
            key = sequence[ sidx ]
            sidx += 1
            if isinstance( key, str ):
                key = [key]
            F   = key[0][0]
            XX  = key[0][1:3]
            YYY = int( key[0][3:6] )
            assert F != '1', "replication factor in read compressed"
            assert F != '3', "table D sequence in read compressed"
            if F == '0':
                # get element we are reading
                element = self.table_B_tmp[self.table_B_tmp['FXY'] == key[0]]
                element.reset_index(inplace=True)
                assert element.shape[0] == 1
                scale = float(element.loc[0, 'BUFR_Scale'])
                width = element.loc[0, 'BUFR_DataWidth_Bits']
                reference = float(element.loc[0, 'BUFR_ReferenceValue'])
                unit = element.loc[0, 'BUFR_Unit']
                name = element.loc[0, 'ElementName_en']
                # now read R, B, I1, In where R = reference, B = bits per increment, I = increment
                if (bits[self.idx:(self.idx + width)]).all() and width > 1:
                    local_reference = None
                else:
                    local_reference = int(bits[self.idx:(self.idx + width)].to01(), 2)
                self.idx += width
                if unit != 'CCITT IA5':
                    bits_per_increment =  int(bits[self.idx:(self.idx + 6)].to01(), 2)
                    self.idx += 6
                    if bits_per_increment > 0:
                        for i in range( nsubsets ):
                            if (bits[self.idx:(self.idx + bits_per_increment)]).all() and width > 1:
                                val = None
                            else:
                                if unit == 'Flag table':
                                    val = int(bits[self.idx:(self.idx + bits_per_increment)].to01(),2)
                                    val = local_reference + val
                                    val = format( int(val), 'b').zfill( width )
                                else:
                                    val = int(bits[self.idx:(self.idx + bits_per_increment)].to01(), 2)
                                if unit not in ['CCITT IA5', 'Code table', 'Flag table']:
                                    val = (val + local_reference) * pow(10, -scale)
                            #if key[0] == '002048':
                            #    print( local_reference )
                            #    print( (bits[self.idx:(self.idx + bits_per_increment)]) )
                            subsets.loc[ (i, sidx),:] = [name, key[0], val, unit]#, width, scale, reference]
                            self.idx += bits_per_increment
                    else:
                        #print("{} local ref: {}".format(key, local_reference))
                        for i in range( nsubsets ):
                            if local_reference is not None :
                                val = local_reference * pow(10, -scale)
                            else:
                                val = None
                            subsets.loc[(i, sidx), :] = [name, key[0], val, unit]#, width, scale, reference]
                else:
                    assert False, "unsupported character compression"
            elif F == '2':
                if XX == '01':  # add YYY - 128 bits to width other than CCITT IA5, code and flag tables
                    exclude = ['CCITT IA5', 'Code table', 'Flag table']
                    mask = ~ self.table_B_tmp['BUFR_Unit'].isin(exclude)
                    if YYY > 0:
                        self.table_B_tmp.loc[mask, 'BUFR_DataWidth_Bits'] = \
                            self.table_B_tmp.loc[mask, 'BUFR_DataWidth_Bits'] + int(YYY) - 128
                    else:
                        self.table_B_tmp.loc[mask, 'BUFR_DataWidth_Bits'] = self.table_B.loc[mask, 'BUFR_DataWidth_Bits'].copy()
                elif XX == '02':  # add YYY - 128 bits to scale other than CCITT IA5, code and flag tables
                    exclude = ['CCITT IA5', 'Code table', 'Flag table']
                    mask = ~ self.table_B_tmp['BUFR_Unit'].isin(exclude)
                    if YYY > 0:
                        self.table_B_tmp.loc[mask, 'BUFR_Scale'] = \
                            self.table_B_tmp.loc[mask, 'BUFR_Scale'] + int(YYY) - 128
                    else:
                        self.table_B_tmp.loc[mask, 'BUFR_Scale'] = self.table_B.loc[mask, 'BUFR_Scale'].copy()
                elif XX == '08':  # change width of CCITT IA5 field to YYY bits
                    include = ['CCITT IA5']
                    mask = self.table_B_tmp['BUFR_Unit'].isin(include)
                    if YYY > 0:
                        self.table_B_tmp.loc[mask, 'BUFR_DataWidth_Bits'] = YYY * 8
                    else:
                        self.table_B_tmp.loc[mask, 'BUFR_DataWidth_Bits'] = self.table_B.loc[mask, 'BUFR_DataWidth_Bits'].copy()
                else:
                    print('Unsupported operator')
                    assert False
        return( subsets )



    def read_sequence(self, sequence, bits, reset = True):
        # arguments
        assert isinstance(sequence, list)
        # create empty data frame to store elements being read
        subset = pd.DataFrame( {'ElementName':[],'FXY':[],'Value':[],'Units':[]} )
        if reset:
            self.table_B_tmp = self.table_B.copy()
        sequence_length = len( sequence )
        sidx = 0
        associated_field_length = 0
        while sidx < sequence_length :
            key = sequence[ sidx ]
            # force key to be a list
            if isinstance( key, str ):
                key = [key]
            F   = key[0][0]
            XX  = key[0][1:3]
            YYY = int( key[0][3:6] )
            # identify what we are processing (element, operator, replication of D sequence)
            if F == '1':
                nelements_to_repeat = int( XX )
                nreplications = int( YYY )
                if nreplications == 0:
                    sidx += 1
                    # next element should be class 31
                    key = sequence[sidx]
                    # force key to be a list
                    if isinstance(key, str):
                        key = [key]
                    F = key[0][0]
                    XX = key[0][1:3]
                    YYY = key[0][3:6]
                    assert XX == '31', 'XX != 31'
                    tmp_df = self.read_sequence( key, bits, reset=False)
                    sidx += 1
                    subset = pd.concat([subset, tmp_df])
                    if tmp_df['Value'][0] != None:
                        nreplications = int( tmp_df['Value'][0] )
                    else:
                        nreplications = 0
                else:
                    sidx += 1
                subsequence = sequence[sidx:(sidx+nelements_to_repeat)]
                sidx += nelements_to_repeat
                if nreplications > 0:
                    for repeatition in range(nreplications):
                        tmp_df = self.read_sequence(subsequence, bits, reset=False)
                        subset = pd.concat([subset, tmp_df])
            elif F == '2':
                if XX == '01':  # add YYY - 128 bits to width other than CCITT IA5, code and flag tables
                    exclude = ['CCITT IA5', 'Code table', 'Flag table']
                    mask = ~ self.table_B_tmp['BUFR_Unit'].isin(exclude)
                    if YYY > 0:
                        self.table_B_tmp.loc[mask, 'BUFR_DataWidth_Bits'] = \
                            self.table_B_tmp.loc[mask, 'BUFR_DataWidth_Bits'] + int(YYY) - 128
                    else:
                        self.table_B_tmp.loc[mask, 'BUFR_DataWidth_Bits'] = self.table_B.loc[mask, 'BUFR_DataWidth_Bits'].copy()
                elif XX == '02':  # add YYY - 128 bits to scale other than CCITT IA5, code and flag tables
                    exclude = ['CCITT IA5', 'Code table', 'Flag table']
                    mask = ~ self.table_B_tmp['BUFR_Unit'].isin(exclude)
                    if YYY > 0:
                        self.table_B_tmp.loc[mask, 'BUFR_Scale'] = \
                            self.table_B_tmp.loc[mask, 'BUFR_Scale'] + int(YYY) - 128
                    else:
                        self.table_B_tmp.loc[mask, 'BUFR_Scale'] = self.table_B.loc[mask, 'BUFR_Scale'].copy()
                elif XX == '08':  # change width of CCITT IA5 field to YYY bits
                    include = ['CCITT IA5']
                    mask = self.table_B_tmp['BUFR_Unit'].isin(include)
                    if YYY > 0:
                        self.table_B_tmp.loc[mask, 'BUFR_DataWidth_Bits'] = YYY * 8
                    else:
                        self.table_B_tmp.loc[mask, 'BUFR_DataWidth_Bits'] = self.table_B.loc[
                            mask, 'BUFR_DataWidth_Bits'].copy()
                elif XX == '04': # add associated field
                    # Precede each element with YYY bits of information.
                    # This operation associates a data field (e.g. quality indicator) of YYY bits with each
                    # data element
                    associated_field_length = int( YYY )
                else:
                    print('Unsupported operator')
                    assert False
                tmp_df = pd.DataFrame({
                    'ElementName': ['Operator'],
                    'FXY': [key[0]],
                    'Value': [None],
                    'Units': [None]
                })
                subset = pd.concat([subset, tmp_df])
                sidx += 1
            elif F == '3':
                subsequence = list(self.table_D.loc[self.table_D['FXY1'] == key[0], 'FXY2'].copy())
                sidx += 1
                tmp_df = self.read_sequence(subsequence, bits, reset=False)
                subset = pd.concat([subset, tmp_df])
            else:
                assert F == '0', 'F != 0'
                # identify what we are reading
                element = self.table_B_tmp[self.table_B_tmp['FXY'] == key[0]]
                element.reset_index(inplace=True)
                assert element.shape[0] == 1 ,'shape != 1'
                scale = float(element.loc[0, 'BUFR_Scale'])
                width = element.loc[0, 'BUFR_DataWidth_Bits']
                reference = float(element.loc[0, 'BUFR_ReferenceValue'])
                unit = element.loc[0, 'BUFR_Unit']
                name = element.loc[0, 'ElementName_en']
                # check if associated field length > 0
                if (associated_field_length > 0) and (unit not in ['CCITT IA5', 'Code table', 'Flag table']):
                    val = int(bits[self.idx:(self.idx + associated_field_length)].to01(),2)
                    self.idx += associated_field_length
                    tmp_df = pd.DataFrame({'ElementName': ["Associated field"], 'FXY': [""], 'Value': [val], 'Units': [""]})
                    subset = pd.concat([subset, tmp_df])
                # now read value
                if (bits[self.idx:(self.idx + width)]).all() and width > 1:
                    val = None
                else:
                    if unit == 'CCITT IA5':
                        val = bits[self.idx:(self.idx + width)].tobytes().decode('ascii')
                        val = val.strip()
                    else:
                        val = int(bits[self.idx:(self.idx + width)].to01(), 2)
                        if unit not in ['CCITT IA5', 'Code table', 'Flag table']:
                            val = (val + reference) * pow(10, -scale)
                #print( '{}: {}, {}'.format(name, bits[self.idx:(self.idx + width)], val) )
                self.idx += width
                tmp_df = pd.DataFrame({'ElementName': [name], 'FXY': [key[0]], 'Value': [val], 'Units': [unit]})
                subset = pd.concat([subset, tmp_df])
                sidx += 1
        return( subset )

    def read_section0(self, bits):
        assert len(bits) == 64, 'Section 0 bad length'
        bufr = bits[0:32].tobytes().decode('ascii')
        assert bufr == 'BUFR', 'BUFR magic number not found'
        length = int(bits[33:56].to01(), 2)
        edition = int(bits[56:64].to01(), 2)
        assert edition == 4, 'Only BUFR edition 4 supported'
        section0 = {'bufr': bufr, 'length': length, 'version': edition}
        self.section0 = section0

    def read_section1(self, bits):
        length = int(bits[0:24].to01(), 2)
        assert len(bits) == length * 8, 'Bad length in section 1'
        master_table = int(bits[24:32].to01(), 2)
        originating_centre = int(bits[32:48].to01(), 2)
        sub_centre = int(bits[48:64].to01(), 2)
        update_sequence = int(bits[64:72].to01(), 2)
        optional_section = bits[72:80]
        data_category = int(bits[80:88].to01(), 2)
        international_sub_category = int(bits[88:96].to01(), 2)
        local_sub_category = int(bits[96:104].to01(), 2)
        master_table_version = int(bits[104:112].to01(), 2)
        local_table_version = int(bits[112:120].to01(), 2)
        year = int(bits[120:136].to01(), 2)  # 2
        month = int(bits[136:144].to01(), 2)
        day = int(bits[144:152].to01(), 2)
        hour = int(bits[152:160].to01(), 2)
        minute = int(bits[160:168].to01(), 2)
        second = int(bits[168:176].to01(), 2)

        optional_length = (length - 22) * 8
        if optional_length > 0:
            optional = int(bits[176:(176 + optional_length)].to01(), 2)
        else:
            optional = None

        section1 = {'master_table': master_table,
                    'originating_centre': originating_centre,
                    'sub_centre': sub_centre,
                    'update_sequence': update_sequence,
                    'optional_section': optional_section,
                    'data_category': data_category,
                    'international_sub_category': international_sub_category,
                    'local_sub_category': local_sub_category,
                    'master_table_version': master_table_version,
                    'local_table_version': local_table_version,
                    'year': year,
                    'month': month,
                    'day': day,
                    'hour': hour,
                    'minute': minute,
                    'second': second,
                    'optional': optional}
        self.section1 = section1

    def read_section2(self, bits):
        length = int(bits[0:24].to01(), 2)
        assert len(bits) == length * 8
        zero = int(bits[24:32].to01(), 2)
        assert zero == 0
        length_local = (length - 4) * 8
        if length_local > 0:
            local_use = bits[32:(32 + length_local)]
        else:
            local_use = None
        section2 = {'length': length, 'zero': zero, 'local_use': local_use}
        self.section2 = section2

    def read_section3(self, bits):
        length = int(bits[0:24].to01(), 2)
        assert len(bits) == length * 8, 'Bad length in section 3'
        zero = int(bits[24:32].to01(), 2)
        assert zero == 0, 'Zero != 0 in section 3'
        number_subsets = int(bits[32:48].to01(), 2)
        flags = int(bits[48:56].to01(), 2)
        #assert flags != 192, "compressed data not yet supported"
        ndescriptors = int(((length - 7) / 2))
        idx = 56
        unexpanded_descriptors = list()
        for i in range(ndescriptors):
            if (idx + 16) > len(bits):
                print("warning 1")
                break
            F = int(bits[idx:(idx + 2)].to01(), 2)
            XX = int(bits[idx + 2:(idx + 8)].to01(), 2)
            YYY = int(bits[idx + 8:(idx + 16)].to01(), 2)
            descriptor = '{0:01d}{1:02d}{2:03d}'.format(F, XX, YYY)
            unexpanded_descriptors.append(descriptor)
            idx += 16


        section3 = {
            'length': length,
            'zero': zero,
            'number_subsets': number_subsets,
            'flags': flags,
            'ndescriptors': ndescriptors,
            'unexpanded_descriptors': unexpanded_descriptors
        }
        self.section3 = section3

    def read_section4(self, bits):
        length = int(bits[0:24].to01(), 2)
        assert len(bits) == length * 8, 'Bad length in section 4'
        zero = int(bits[24:32].to01(), 2)
        assert zero == 0, 'Zero != 0 in section 4'
        length_payload = length * 8 - 32
        payload = bits[32:(32 + length_payload)]
        section4 = {
            'length': length,
            'zero': zero,
            'payload': payload
        }
        self.section4 = section4

    def read_section5(self, bits):

        if( len( bits ) == 0):
            print("Warning, section 5 missing")
            section5 = {'sevens': '7777'}
            self.section5 = section5
        else:
            assert len(bits) == 4 * 8, 'Bad length in section 5'
            sevens = bits[0:32].tobytes().decode('ascii')
            assert sevens == '7777', 'No sevens in Section 5'
            section5 = {'sevens': sevens}
            self.section5 = section5
        print('section 5 read')

    def read_header(self, bits):
        idx = 0
        section_length = 64
        self.read_section0(bits[idx:idx + section_length])
        idx = idx + section_length
        section_length = int(bits[idx:(idx + 24)].to01(), 2) * 8
        self.read_section1(bits[idx:(idx + section_length)])
        idx = idx + section_length
        if self.section1['optional_section'][0]:
            section_length = int(bits[idx:(idx + 24)].to01(), 2) * 8
            self.read_section2(bits[idx:(idx + section_length)])
            idx = idx + section_length
        else:
            section2 = None
        section_length = int(bits[idx:(idx + 24)].to01(), 2) * 8
        self.read_section3(bits[idx:(idx + section_length)])

    def read_message(self, bits):
        idx = 0
        section_length = 64
        self.read_section0(bits[idx:idx + section_length])
        #print( self.section0 )
        idx += section_length
        section_length = int(bits[idx:(idx + 24)].to01(), 2) * 8
        self.read_section1(bits[idx:(idx + section_length)])
        #print(self.section1)
        idx += section_length
        if self.section1['optional_section'][0]:
            section_length = int(bits[idx:(idx + 24)].to01(), 2) * 8
            self.read_section2(bits[idx:(idx + section_length)])
            idx = idx + section_length
        else:
            section2 = None
        section_length = int(bits[idx:(idx + 24)].to01(), 2) * 8
        self.read_section3(bits[idx:(idx + section_length)])
        #print( self.section3 )
        idx += section_length
        section_length = int(bits[idx:(idx + 24)].to01(), 2) * 8
        self.read_section4(bits[idx:(idx + section_length)])
        idx += section_length
        section_length = 4 * 8
        self.read_section5(bits[idx:(idx + section_length)])


