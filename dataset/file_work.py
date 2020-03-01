
file_names = ["aka_name", "aka_title", "cast_info", "char_name", "company_name",
              "movie_companies", "name", "title", "movie_info", "person_info"]

for file_name in file_names:

    output_f = open(file_name + "_new.csv", "w")

    with open(file_name + ".csv", "r") as input_f:

        for idx, line in enumerate(input_f):

            text = line.split(',')
            line_len = len(text)

            if file_name == "aka_name":
                # aka_name, columns: 8 -- replace commas in splits range(2, len-5)
                # text[line_len - 5] values: |, ||, |||
                text_field = ""
                for i in range(2, line_len-5):
                    text_field += text[i]
                text_field = text_field.replace("\"", "")

                parsed_text = text[0] + "," + text[1] + "," + text_field + "," + text[line_len - 5] + "," \
                    + text[line_len - 4] + "," + text[line_len - 3] + "," \
                    + text[line_len - 2] + "," + text[line_len - 1]
                output_f.write(parsed_text)
            elif file_name == "aka_title":
                # aka_title, columns: 12 -- replace commas in splits range(2, len-9)
                # two columns are text fields. Column #11 doesn't have commas
                text_field = ""
                for i in range(2, line_len-9):
                    text_field += text[i]
                text_field = text_field.replace("\"", "")

                parsed_text = text[0] + "," + text[1] + "," + text_field + "," + text[line_len - 9] + "," \
                    + text[line_len - 8] + "," + text[line_len - 7] + "," + text[line_len - 6] + "," \
                    + text[line_len - 5] + "," + text[line_len - 4] + "," + text[line_len - 3] + "," \
                    + text[line_len - 2] + "," + text[line_len - 1]
                output_f.write(parsed_text)
            elif file_name == "cast_info":
                # cast_info, columns: 7 -- replace commas in splits range(4, len-2)
                text_field = ""
                for i in range(4, line_len-2):
                    text_field += text[i]
                text_field = text_field.replace("\"", "")

                parsed_text = text[0] + "," + text[1] + "," + text[2] + "," + text[3] + "," \
                    + text_field + "," + text[line_len - 2] + "," + text[line_len - 1]
                output_f.write(parsed_text)
            elif file_name == "char_name":
                # char_name, columns: 7 -- replace commas in splits range(1, len-5)
                text_field = ""
                for i in range(1, line_len-5):
                    text_field += text[i]
                text_field = text_field.replace("\"", "")

                parsed_text = text[0] + "," + text_field + "," + text[line_len - 5] + "," \
                    + text[line_len - 4] + "," + text[line_len - 3] + "," + text[line_len - 2] + "," \
                    + text[line_len - 1]
                output_f.write(parsed_text)
            elif file_name == "company_name":
                # company_name, columns: 7 -- replace commas in splits range(1, len-5)
                text_field = ""
                for i in range(1, line_len-5):
                    text_field += text[i]
                text_field = text_field.replace("\"", "")

                parsed_text = text[0] + "," + text_field + "," + text[line_len - 5] + "," + text[line_len - 4] + "," \
                    + text[line_len - 3] + "," + text[line_len - 2] + "," + text[line_len - 1]
                output_f.write(parsed_text)
            elif file_name == "movie_companies":
                # movie_companies, columns: 5 -- replace commas in splits range(4, len)
                text_field = ""
                for i in range(4, line_len):
                    text_field += text[i]
                text_field = text_field.replace("\"", "")

                parsed_text = text[0] + "," + text[1] + "," + text[2] + "," + text[3] + "," + text_field
                output_f.write(parsed_text)
            elif file_name == "name":
                # name, columns: 9 -- replace commas in splits range(1, len-7)
                text_field = ""
                for i in range(1, line_len-7):
                    text_field += text[i]
                text_field = text_field.replace("\"", "")

                parsed_text = text[0] + "," + text_field + "," + text[line_len - 7] + "," \
                    + text[line_len - 6] + "," + text[line_len - 5] + "," + text[line_len - 4] + "," \
                    + text[line_len - 3] + "," + text[line_len - 2] + "," + text[line_len - 1]
                output_f.write(parsed_text)
            elif file_name == "title":
                # title, columns: 12 -- replace commas in splits range(1, len-10)
                text_field = ""
                for i in range(1, line_len-10):
                    text_field += text[i]
                text_field = text_field.replace("\"", "")

                parsed_text = text[0] + "," + text_field + "," + text[line_len - 10] + "," \
                    + text[line_len - 9] + "," + text[line_len - 8] + "," + text[line_len - 7] + "," \
                    + text[line_len - 6] + "," + text[line_len - 5] + "," + text[line_len - 4] + "," \
                    + text[line_len - 3] + "," + text[line_len - 2] + "," + text[line_len - 1]
                output_f.write(parsed_text)
            elif file_name == "movie_info":
                # movie_info, columns: 5 -- replace commas in splits range(3, len-1)
                # 1436974 tuples with non-empty last column
                temp = ""
                for i in range(3, line_len):
                    temp += "," + text[i]
                temp = temp[1:]
                temp = temp.replace("\"", "")
                temp = temp.split(",")

                rest_text = temp[len(temp) - 1]
                text_field = ""
                for i in range(0, len(temp) - 1):  # removing " signs
                    text_field += temp[i]

                parsed_text = text[0] + "," + text[1] + "," + text[2] + "," + text_field + "," + rest_text
                output_f.write(parsed_text)
            elif file_name == "person_info":
                # person_info, columns: 5 -- replace commas in splits range(3, len-1)
                # 84183 tuples with non-empty last column
                temp = ""
                for i in range(3, line_len):
                    temp += "," + text[i]
                temp = temp[1:]
                temp = temp.replace("\"", "")
                temp = temp.split(",")

                rest_text = temp[len(temp) - 1]
                text_field = ""
                for i in range(0, len(temp) - 1):  # removing " signs
                    text_field += temp[i]
                if len(text_field) > 32767:  # very long string data
                    text_field = text_field[:32700]

                parsed_text = text[0] + "," + text[1] + "," + text[2] + "," + text_field + "," + rest_text
                output_f.write(parsed_text)

    output_f.close()
