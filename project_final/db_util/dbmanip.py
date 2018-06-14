import sqlite3



def connect_db(db_name, logger):
    try:
        conn = sqlite3.connect(db_name + '.db')
        logger.info(f'Connetion stablished with DB: {db_name}.db')

        return conn


    except sqlite3.OperationalError:
        logger.error(f'Could not connect with {db_name}.db. Make sure the DB name is right')


def create_table(conn, logger):
    c = conn.cursor()

    try:
        c.execute('CREATE TABLE IF NOT EXISTS chipseq(cell_type_category TEXT NOT NULL,'
                  ' cell_type TEXT NOT NULL,'
                  ' cell_type_track_name TEXT NOT NULL,'
                  ' cell_type_short TEXT NOT NULL,'
                  ' assay_category TEXT NOT NULL,'
                  ' assay TEXT NOT NULL,'
                  ' assay_track_name TEXT NOT NULL,'
                  ' assay_short TEXT NOT NULL,'
                  ' donor TEXT NOT NULL,'
                  ' time_point TEXT NOT NULL,'
                  ' view TEXT NOT NULL,'
                  ' track_name TEXT NOT NULL,'
                  ' track_type TEXT NOT NULL,'
                  ' track_density TEXT NOT NULL,'
                  ' provider_institution TEXT NOT NULL,'
                  ' source_server TEXT NOT NULL,'
                  ' source_path_to_file TEXT NOT NULL,'
                  ' server TEXT NOT NULL,'
                  ' path_to_file TEXT NOT NULL,'
                  ' new_file_name TEXT NOT NULL);')

        logger.info('Table chipseq was created')

    except sqlite3.OperationalError:
        logger.error('Table chipseq could not be created')


def insert_data(conn, list_of_data, logger):
    c = conn.cursor()

    try:
        with conn:
            for data in list_of_data:
                c.execute("INSERT INTO chipseq VALUES(:cell_type_category, :cell_type, :cell_type_track_name,"
                          ":cell_type_short, :assay_category, :assay, :assay_track_name,"
                          ":assay_short, :donor, :time_point, :view, :track_name, :track_type, :track_density,"
                          ":provider_institution, :source_server, :source_path_to_file, :server, :path_to_file,"
                          ":new_file_name)", data)
            logger.info('Data was inserted on the DB')

    except sqlite3.OperationalError:
        logger.error('Data could not be inserted')


def update_assay(conn, assay, assay_new, logger):

    c = conn.cursor()

    try:
        with conn:
            c.execute("UPDATE chipseq SET assay = :assay_new  WHERE assay = :assay", {'assay': assay, 'assay_new': assay_new})
            logger.info(f'assay:{assay} was updated for assay_new: {assay_new}')

    except sqlite3.OperationalError:
        logger.error(f'COLD NOT UPDATE assay:{assay} for assay_new: {assay_new}')


def update_donor(conn, donor, donor_new, logger):

    c = conn.cursor()

    try:
        with conn:
            c.execute("UPDATE chipseq SET donor = :donor_new  WHERE donor = :donor", {'donor': donor, 'donor_new': donor_new})
            logger.info(f'donor:{donor} was updated for donor_new: {donor_new}')

    except sqlite3.OperationalError:
        logger.error(f'COLD NOT UPDATE donor:{donor} for donor_new: {donor_new}')




def select_cell_types(conn, logger):

    c = conn.cursor()

    try:
        with conn:
            c.execute("SELECT  DISTINCT cell_type FROM chipseq")
            all_cell_types = c.fetchall()

            logger.info(f'Selected cell_type')
            return all_cell_types

    except sqlite3.OperationalError:
        logger.error(f'Could not Select cell_types. Check if the table exists.')


def select_assays(conn, assays, logger):
    c = conn.cursor()

    try:
        with conn:
            c.execute("SELECT DISTINCT track_name, track_type, track_density FROM chipseq WHERE assay = :assays", {"assays": assays})
            all_assays = c.fetchall()

            logger.info(f'Selected chipseq with assay: {assays}')

            return all_assays

    except sqlite3.OperationalError:
        logger.error(f'Could not Select chipseq assays. Check if the table exists.')


def select_assay_track_names(conn, assay_track_name, logger):
    c = conn.cursor()

    try:
        with conn:
            c.execute("SELECT DISTINCT track_name FROM chipseq WHERE assay_track_name = :assay_track_name", {"assay_track_name": assay_track_name})
            all_assay_track_names = c.fetchall()

            logger.info(f'Selected chipseq with assay_track_name: {assay_track_name}')

            return all_assay_track_names

    except sqlite3.OperationalError:
        logger.error(f'Could not Select chipseq assay_track_name. Check if the table exists.')



def select_assay_cells(conn, assay, logger):
    c = conn.cursor()

    try:
        with conn:
            c.execute("SELECT DISTINCT cell_type FROM chipseq WHERE assay = :assay", {"assay": assay})
            all_assay_cells = c.fetchall()

            logger.info(f'Selected chipseq with assay: {assay}')

            return all_assay_cells

    except sqlite3.OperationalError:
        logger.error(f'Could not Select chipseq assay_cell. Check if the table exists.')



def delete_track_name(conn, track_name, logger):
    c = conn.cursor()

    try:
        with conn:
            c.execute("DELETE FROM chipseq WHERE track_name = :track_name", {"track_name": track_name})

            logger.info(f'Rows where track_name is: "{track_name}",  were deleted')

    except sqlite3.OperationalError:
        logger.error(f'Could not delete {track_name}')