from flask import Flask, render_template, request, Response
from bufr_message import *
import pandas as pd
import bitarray

app = Flask(__name__, template_folder="templates")

@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"

@app.route("/upload/")
def upload():
    return render_template("upload.html")

@app.route("/decode/",methods=["POST"])
def decode():
    if request.method == "POST":
        # first save temporary file
        file = request.files['file']
        file.save("tmp.bufr")
        return_value = "<h2>{}</h2>\n".format(file.filename)
        # ===================================================================
        # now process file
        fh = open("tmp.bufr","rb")
        # bit array to store the data
        bits = bitarray.bitarray()
        # now load the data
        bits.fromfile(fh)
        # close file
        fh.close()
        # find start of BUFR message
        idx = 0
        nbits = len(bits)
        while idx < (nbits - 32):
            if ((bits[idx:(idx + 32)].tobytes() == b'BUFR')):
                break
            idx += 1
        # load tables and initialise bufr message
        bufr_tables = './tables/'
        msg = bufr_message(bufr_tables + '/BUFRCREX_TableB_en.txt',
                           bufr_tables + '/BUFR_TableD_en.txt')

        # now try reading message from bit array
        try:
            msg.read_message(bits[idx:nbits])
        except:
            print("Error reading : {}".format(f))
            print(idx)
            print(nbits)
            #continue

        if msg.section0['version'] != 4:
            print("Only BUFR edition 4 supported, skipping file")
            next

        if msg.section3['flags'] == 192:
            s = msg.expand_sequence(msg.section3['unexpanded_descriptors'])
        else:
            s = None

        # process section 0
        for key in msg.section0:
            return_value += "<p>{}: {}</p>\n".format(key, msg.section0[key])

        # process section 1
        for key in msg.section1:
            return_value += "<p>{}: {}</p>\n".format(key, msg.section1[key])

        # process section 2
        #if msg.section2 is not None:
        #    for key in msg.section2:
        #        return_value += "<p>{}: {}</p>\n".format(key, msg.section2[key])

        # process section 3
        for key in msg.section3:
            return_value += "<p>{}: {}</p>\n".format(key, msg.section3[key])

        # process section 4
        for key in msg.section4:
            if key != "payload":
                return_value += "<p>{}: {}</p>\n".format(key, msg.section4[key])

        # now read the message
        if msg.section3['flags'] == 192:
            # compressed data
        #    subsets = msg.read_compressed_sequence(s, msg.section4['payload'], msg.section3['number_subsets'])
        #    subsets = subsets.sort_index(level=0)
        #    subsets.to_csv('test.txt', sep=',', na_rep='NA')
        #    print("Compressed message, see associated txt file", fh2)
             print("<p>Compressed data not yet supported!</p>")
        else:
            # uncompressed
            s = msg.section3['unexpanded_descriptors']
            # data frame to hold data
            all_subsets = pd.DataFrame()
            # iterate over subsets reading data
            sindex = 0
            for subset in range(msg.section3['number_subsets']):
                # feedback to user
                print('subset {}'.format(subset))
                success = False
                old_idx = msg.idx
                offset = 0
                while (not success):
                    print(msg.idx % 8)
                    try:
                        ss = msg.read_sequence(s, msg.section4['payload'])
                        success = True
                    except:
                        offset = offset + 1
                        msg.idx = old_idx + offset
                        assert (False)
                    assert (offset < 1)
                ss.reset_index(inplace=True, drop=True)
                ss = ss.assign( subset=sindex )
                # grow / add subset to data frame
                all_subsets = pd.concat([all_subsets, ss])
                sindex += 1

        # now sort out indexes and order of columns
        all_subsets = all_subsets.assign( element_number = all_subsets.index )
        all_subsets = all_subsets[['subset','element_number','FXY','ElementName','Value','Units']]
        all_subsets.to_csv("decoded.csv")

        return_value += '<a href="./../download/">Download csv</a>'

        return_value += all_subsets.to_html()
        return( return_value )


@app.route("/download/")
def download():
        name = 'decoded.csv'
        with open(name, 'rb') as f:
            resp = Response( f.read() )
        resp.headers["Content-Disposition"] = "attachment; filename={0}".format(name)
        resp.headers["Content-type"] = "text/csv"
        return( resp )

if __name__ == '__main__':
    print("Running flask app")
    app.run(debug=True, host='0.0.0.0')
