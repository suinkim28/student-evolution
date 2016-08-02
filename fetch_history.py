import pymongo
import sys
import ottype
import config
import psycopg2
import json


global_user_timestamps = {}
global_user_chars_modified = {}
global_user_files = {}

def main():
    # RETRIEVE USERID AND PASSWORD FROM CONFIG
    # CONNECT TO MONGODB
    elice_mongo = mongodb_connect()

    # CONNECT TO POSTGRESQL
    elice_postgres = postgres_connect()

    # FETCH METADATA
    lecture_materials = fetch_metadata(elice_postgres, course_id=config.COURSE['id'])

    # FETCH DOCS
    for lecture in lecture_materials:
        lecture_id = lecture['id']
        lecture_title = lecture['title']
        global_user_files[lecture_id] = {}
        print("=== Lecture ===")
        print("Lecture %d: %s" % (lecture_id, lecture_title))

        for material in lecture['exercises']:
            global_user_files[lecture_id][material['id']] = {}
            print("Exercise %d: %s" % (material['id'], material['title']))
            fetch_docs(elice_mongo, elice_postgres, lecture_id, material_exercise_ids=[material['id']])

    with open("student_commit_files.json", "w+") as fp:
        json.dump(global_user_files, fp)


    '''
    # CALCULATE TIMES SPENT AND SAVE
    cutoff = 1000 * 60 * 60

    fp = open("%d_times.csv" % config.COURSE['id'], "w+")
    fp_chars = open("%d_chars.csv" % config.COURSE['id'], "w+")

    for user_id in global_user_timestamps:
        user_times_spent = 0
        timestamps = sorted(global_user_timestamps[user_id])
        timestamps = [int(x) for x in timestamps]
        for i in range(len(timestamps) - 1):
            between = timestamps[i + 1] - timestamps[i]
            if between < cutoff:
                user_times_spent += between
        fp.write("%d,%.4lf\n" % (user_id, user_times_spent / 1000 / 60 / 60))

    for user_id in global_user_chars_modified:
        fp_chars.write("%d,%d\n" % (user_id, global_user_chars_modified[user_id]))

    fp_chars.close()
    fp.close()
    '''

def mongodb_connect():
    # CONNECT TO MONGODB
    client = pymongo.MongoClient('mongodb://%s:%s@%s/%s?authMechanism=SCRAM-SHA-1' % \
                                 (config.MONGODB['user'],
                                  config.MONGODB['password'],
                                  config.MONGODB['host'],
                                  config.MONGODB['db']), config.MONGODB['port'])
    db = client.elice

    print("=== Collections ===")
    print("\n".join(db.collection_names()))

    return db

def postgres_connect():
    # CONNECT TO POSTGRESQL
    conn = psycopg2.connect("host='%s' dbname='%s' user='%s' password='%s'" % \
                            (config.POSTGRES['host'],
                             config.POSTGRES['db'],
                             config.POSTGRES['user'],
                             config.POSTGRES['password']))
    cur = conn.cursor()

    return cur

def fetch_metadata(elice_postgres, course_id):
    # FETCH LECTURES
    elice_postgres.execute("""SELECT * FROM LECTURE_MODEL
                              WHERE COURSE_ID = %d AND IS_DELETED = False
                              ORDER BY ID""" % \
                           (course_id))
    lectures = []
    for lecture in elice_postgres.fetchall():
        lecture_id = lecture[2]
        lecture_title = lecture[4]
        if 'homework' not in lecture_title.lower(): continue
        print("%d: %s" % (lecture_id, lecture_title))
        lectures.append(lecture)

    # LECTURES AND MATERIAL EXERCISES
    lecture_materials = []

    # FETCH MATERIAL EXERCISES
    for lecture in lectures:
        lecture_id = lecture[2]
        elice_postgres.execute("""SELECT material_exercise_model.id, material_exercise_model.title FROM material_exercise_model
                                  INNER JOIN lecture_page_order_model
                                  ON material_exercise_model.lecture_page_order_id = lecture_page_order_model.id
                                  WHERE lecture_page_order_model.lecture_id = %d
                                  AND material_exercise_model.is_deleted = False
                                  ORDER BY lecture_page_order_model.order_no ASC""" % \
                                  (lecture_id))

        # LIST OF DICTIONARIES
        exercises = [{'id': x[0], 'title': x[1]} for x in elice_postgres.fetchall()]

        # MATERIAL EXERCISES FOR LECTURE_ID
        lecture_materials.append({'id': lecture_id,
                                  'title': lecture[4],
                                  'exercises': exercises})

    print(lecture_materials)
    return lecture_materials


def fetch_docs(elice_mongo, elice_postgres, lecture_id, material_exercise_ids):
    # FILTER BY MATERIAL EXERCISE IDS
    filter_conditions = {
        'material_exercise_id': { '$in': material_exercise_ids }
    }

    print("%d docs found for %s" % (elice_mongo['usercode_share.docs'].count(filter_conditions), str(material_exercise_ids)))
    usercode_docs = elice_mongo['usercode_share.docs'].find(filter_conditions)

    for doc in usercode_docs:
        user_id = doc['owner_id']
        user_commits = elice_postgres.execute( \
                         """
                         SELECT T1.created_datetime, T1.input_data, T1.is_commit, T2.auto_score FROM EXERCISE_RUNNING_LOG_MODEL T1
                         INNER JOIN EXERCISE_RUNNING_RESULT_LOG_MODEL T2
                         ON T2.exercise_running_id = T1.id
                         WHERE
                         T1.user_id = %d
                         AND T1.material_exercise_id = %d
                         ORDER BY T1.created_datetime desc
                         """ % (user_id, material_exercise_ids[0]))
        user_commits = elice_postgres.fetchall();

        global_user_files[lecture_id][material_exercise_ids[0]][user_id] = {}
        fetch_ops(elice_mongo, doc, doc['_id'], user_commits, lecture_id, material_exercise_ids[0], user_id)


def fetch_ops(elice_mongo, doc, doc_id, user_commits, lecture_id, material_exercise_id, user_id):
    # FILTER BY DOCUMENT OBJECT ID
    filter_conditions = {
        'doc_id': doc_id
    }
    ops = elice_mongo['usercode_share.ops'].find(filter_conditions)

    # GET THE LATEST COMMIT
    user_commit_iter = iter(user_commits)
    user_commit = next(user_commit_iter, None)
    # CHECK IF THERE IS A COMMIT...
    if user_commit is None:
        return
    content = doc['content']
    for op in ops:
        striped_op = op['op']
        version = op['version']
        content = ottype.inverse_apply(content, striped_op)
        version_timestamp = op['created_timestamp']
        if version_timestamp <= user_commit[0]:
            # THIS VERSION CORRESPONDS TO CURRENT COMMIT
            global_user_files[lecture_id][material_exercise_id][user_id].setdefault(version, {})
            global_user_files[lecture_id][material_exercise_id][user_id][version]['score'] = user_commit[3]
            global_user_files[lecture_id][material_exercise_id][user_id][version]['timestamp'] = user_commit[0]
            global_user_files[lecture_id][material_exercise_id][user_id][version]['is_submit'] = user_commit[2]
            global_user_files[lecture_id][material_exercise_id][user_id][version].setdefault('files', {})
            global_user_files[lecture_id][material_exercise_id][user_id][version]['files'][doc['filename']] = content

            # PROCEED TO NEXT COMMIT
            user_commit = next(user_commit_iter, None)
            if user_commit is None:
                break


def fetch_docs_stats(elice_mongo, elice_postgres, material_exercise_ids):
    # FILTER BY MATERIAL EXERCISE IDS
    filter_conditions = {
        'material_exercise_id': { '$in': material_exercise_ids }
    }

    print("=== Documents ===")
    print("%d docs found " % (elice_mongo['usercode_share.docs'].count(filter_conditions)))
    usercode_docs = elice_mongo['usercode_share.docs'].find(filter_conditions)

    for doc in usercode_docs:
        user_id = doc['owner_id']
        ops = fetch_ops_all(elice_mongo, doc, doc['_id'])
        global_user_timestamps.setdefault(user_id, [])
        global_user_chars_modified.setdefault(user_id, 0)
        for op in ops:
            global_user_timestamps[user_id].append(op['created_timestamp'])
            striped_op = op['op']
            for unit_op in striped_op:
                if isinstance(unit_op, str):
                    global_user_chars_modified[user_id] += len(unit_op)
                elif isinstance(unit_op, dict):
                    global_user_chars_modified[user_id] += len(unit_op['d'])

def fetch_ops_all(elice_mongo, doc, doc_id):
    # FILTER BY DOCUMENT OBJECT ID
    filter_conditions = {
        'doc_id': doc_id
    }
    ops = elice_mongo['usercode_share.ops'].find(filter_conditions)
    return ops

if __name__ == '__main__':
    main()
