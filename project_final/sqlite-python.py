import argparse
import logging
import os
import sys
from db_util import dbmanip as db
import util.loggerinitializer as utl

# Initialize log object
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
utl.initialize_logger(os.getcwd(), logger)


def main():
    parser = argparse.ArgumentParser(description="A Tool manipulate a sqlite DB")

    subparsers = parser.add_subparsers(title='actions',
                                       description='valid actions',
                                       help='Use sqlite-python.py {action} -h for help with each action',
                                       dest='command'
                                       )

    parser_index = subparsers.add_parser('createdb', help='Create database and tables')

    parser_index.add_argument("--db", dest='db', default=None, action="store", help="The DB name",
                        required=True)

    parser_insert = subparsers.add_parser('insert', help='Insert data on tables')

    parser_insert.add_argument("--file",  default=None, action="store", help="TSV file with the data to be inserted",
                        required=True)

    parser_insert.add_argument("--db", default=None, action="store", help="The DB name",
                        required=True)


    parser_update = subparsers.add_parser('update', help='Update a field in a db')

    parser_update.add_argument("--db", default=None, action="store", help="The DB name",
                        required=True)

    parser_update.add_argument("--a", default=False, action="store", help="assay",
                        required=False)

    parser_update.add_argument("--an", default=False, action="store", help="assay new",
                        required=False)

    parser_update.add_argument("--d", default=False, action="store", help="donor",
                               required=False)

    parser_update.add_argument("--dn", default=False, action="store", help="donor new",
                               required=False)



    parser_select = subparsers.add_parser('select', help='Select  fields from the db')

    parser_select.add_argument("--db", default=None, action="store", help="The DB name",
                               required=True)

    parser_select.add_argument("--ct", default=False, action="store_true", help="Select all cell_types",
                               required=False)


    parser_select.add_argument("-a", default=False, action="store",
                               help="Select all assays, track_name, track_type, track_densit and date of a given chipseq with the defined assays",
                               required=False)

    parser_select.add_argument("-atn", default=False, action="store",
                               help="Select track_name, and date of a given chipseq with the defined assay_track_name",
                               required=False)

    parser_select.add_argument("-ac", default=False, action="store",
                               help="Select cell_type, and date of a given chipseq with the defined assay_cell",
                               required=False)


    parser_delete = subparsers.add_parser('delete', help='delete rows from the db')

    parser_delete.add_argument("-tn", default=None, action="store", help="Delete rows where this  appears",
                        required=False)

    parser_delete.add_argument("--db", default=None, action="store", help="The DB name",
                        required=True)


    args = parser.parse_args()
    # print(args)

    # sys.exit()
    conn = db.connect_db(args.db, logger)

    if args.command == "createdb":

        db.create_table(conn, logger)


    elif args.command == "insert":
        list_of_data = []

        with open(args.file, 'r') as f:
            for line in f:

                # reset dictionary
                line_dict = dict()

                # Skip empty lines
                if not line.strip():
                    continue
                if line.startswith(','):
                    continue

                # split line
                values = line.strip().split(',')

                # put each field in a dict
                line_dict['cell_type_category'] = values[0]
                line_dict['cell_type'] = values[1]
                line_dict['cell_type_track_name'] = values[2]
                line_dict['cell_type_short'] = values[3]
                line_dict['assay_category'] = values[4]
                line_dict['assay'] = values[5]
                line_dict['assay_track_name'] = values[6]
                line_dict['assay_short'] = values[7]
                line_dict['donor'] = values[8]
                line_dict['time_point'] = values[9]
                line_dict['view'] = values[10]
                line_dict['track_name'] = values[11]
                line_dict['track_type'] = values[12]
                line_dict['track_density'] = values[13]
                line_dict['provider_institution'] = values[14]
                line_dict['source_server'] = values[15]
                line_dict['source_path_to_file'] = values[16]
                line_dict['server'] = values[17]
                line_dict['path_to_file'] = values[18]
                line_dict['new_file_name'] = values[19]


                #append the dict to a list
                list_of_data.append(line_dict)

        db.insert_data(conn, list_of_data, logger)


    elif args.command == "update" and args.a is not False:
        db.update_assay(conn, args.a, args.an, logger)

    elif args.command == "update" and args.d is not False:
        db.update_donor(conn, args.d, args.dn, logger)



    elif args.command == "select" and args.ct is not False:
        all_cell_types = db.select_cell_types(conn, logger)

        for cell_type in all_cell_types:
            print(cell_type[0])



    elif args.command == "select" and args.a is not False:
        all_assays = db.select_assays(conn, args.a, logger)

        print("\n| track_name\t| track_type\t| track_density")
        for assays in all_assays:
            print("|","\t| ".join(assays))


    elif args.command == "select" and args.atn is not False:
        all_assay_track_names = db.select_assay_track_names(conn, args.atn, logger)

        print("\n| track_name")
        for assay_track_name in all_assay_track_names:
            print("|","\t| ".join(assay_track_name))


    elif args.command == "select" and args.ac is not False:
        all_assay_cells = db.select_assay_cells(conn, args.ac, logger)

        print("\n| cell_type")
        for assay_cell in all_assay_cells:
            print("|","\t| ".join(assay_cell))


    elif args.command == "delete":
        db.delete_track_name(conn, args.tn, logger)


if __name__ == '__main__':
    main()




