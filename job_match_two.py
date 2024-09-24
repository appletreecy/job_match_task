import csv
import sqlite3
from multiprocessing import Pool


class JobSeeker:
    """
    A class to represent a jobseeker.
    """

    def __init__(self, id, name, skills):
        """
        Initialize a JobSeeker instance.

        Parameters:
        - id (int): The jobseeker's ID
        - name (str): The jobseeker's name.
        - skill (str): A comma separated string of jobseeker's skills. 
        """
        self.id = id
        self.name = name
        self.skills = set(skill.strip().lower() for skill in skills.split(','))


class Job:
    """
    A class to represent a job.
    """

    def __init__(self, id, title, required_skills):
        """
        Initialize a Job instance.

        Parameters:
        - id (int): The job's ID.
        - title (str): The job's title.
        - required_skills (str): A comma separated string of required skills.
        """
        self.id = id
        self.title = title
        self.required_skills = set(skill.strip().lower()
                                   for skill in required_skills.split(','))


def read_csv(file_path, cls):
    """
    Read data from a CSV file and create a list of objects of spefified class type

    Parameters:
    - file_path (str): The path to the CSV file.
    - cls: The class type to instantiate objects.

    Returns:
    - List: A list of instances of specified class type.
    """
    data = []
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if cls == JobSeeker:
                jobseeker = JobSeeker(row['id'], row['name'], row['skills'])
                data.append(jobseeker)
            elif cls == Job:
                job = Job(row['id'], row['title'], row['required_skills'])
                data.append(job)
    return data


def create_table(conn):
    """
    Create the jobseekers and jobs tables in the database.

    Parameters:
    - conn (sqlite3.Connection): An SQLite database connections.
    """
    with conn:
        conn.execute('''
                     CREATE TABLE IF NOT EXISTS jobseekers(
                         id INTEGER PRIMARY KEY,
                         name TEXT NOT NULL,
                         skills TEXT NOT NULL
                     )
                     ''')

        conn.execute('''
                     CREATE TABLE IF NOT EXISTS jobs(
                         id INTEGER PRIMARY KEY, 
                         title TEXT NOT NULL,
                         required_skills TEXT NOT NULL
                     )
                     ''')

        # Add indexes for fields that will be queried frequently
        conn.execute(
            'CREATE INDEX IF NOT EXISTS idx_skills ON jobseekers(skills)')
        conn.execute(
            'CREATE INDEX IF NOT EXISTS idx_required_skills ON jobs(required_skills)')


def insert_data(conn, jobseekers, jobs):
    """
    Insert jobseeker and job data into the database.

    Parameters:
    - conn (sqlite3.Connection): An SQLite database connections.
    - jobseekers(List[JobSeeker]): A list of Jobseeker instances.
    - jobs (list[Job]): A list of Job instances. 
    """
    with conn:
        conn.executemany('INSERT INTO jobseekers(id, name, skills) VALUES (?,?,?)', [
                         (js.id, js.name, ",".join(js.skills)) for js in jobseekers])
        conn.executemany('INSERT INTO jobs(id, title, required_skills) VALUES (?,?,?)', [
                         (job.id, job.title, ",".join(job.required_skills)) for job in jobs])


def match_skills(js_skills, job_skills):
    """
    Match skills between a jobseeker and a job.

    Parameters:
    - js_skills (Set[str]): A set of jobseeker's skills.
    - job_skills (Set[str]): A set of job's required skills. 

    Returns:
    - Tuple[int, float]: A tuple containing the number of matching skills and the percentage of matching skills.
    """
    matching_skills = js_skills & job_skills
    match_count = len(matching_skills)
    total_required_skills = len(job_skills)
    match_percent = (match_count / total_required_skills) * \
        100 if total_required_skills > 0 else 0
    return match_count, match_percent


def process_recommendations(js_data, job_data):
    """
    Porcess job recommendations for a jobseeker.

    Parameters:
    - js_data (dict): A dictionary containing jobseeker data.
    - job_data (List[dict]): A List of dictionaries containing job recommendations. 
    """
    recommendations = []

    js_skills = set(js_data['skills'].split(','))
    for job in job_data:
        job_skills = set(job['required_skills'].split(','))
        match_count, match_percent = match_skills(js_skills, job_skills)
        recommendations.append({
            'jobseeker_id': js_data['id'],
            'jobseeker_name': js_data['name'],
            'job_id': job['id'],
            'job_title': job['title'],
            'matching_skill_count': match_count,
            'matching_skill_percent': match_percent

        })
    return recommendations


def generate_recommendation_parallel(conn):
    """
    Generate job recommendations for all jobseekers using parallel processing. 

    Parameters:
    - conn (sqlite3.Connection): An SQLite database connection.

    Returns:
    - List[dict]: A sorted list of job recommendations.
    """
    jobseekers = conn.execute(
        'SELECT id, name, skills FROM jobseekers').fetchall()
    jobs = conn.execute(
        'SELECT id, title, required_skills FROM jobs').fetchall()

    # Convert sqlite3.Row objects to dictionaries
    jobseekers = [dict(js) for js in jobseekers]
    jobs = [dict(job) for job in jobs]

    # Use multiprocessing Pool for parallel processing
    with Pool() as pool:
        results = pool.starmap(process_recommendations, [
                               (js, jobs) for js in jobseekers])

    # Flattern the list of lists
    recommendations = [rec for sublist in results for rec in sublist]

    sorted_recommendations = sorted(recommendations, key=lambda x: (
        x['jobseeker_id'], -x['matching_skill_percent'], x['job_id']))

    return sorted_recommendations


def main():
    """
    Main function to run the job matching application.
    """
    jobseekers = read_csv('jobseekers.csv', JobSeeker)
    jobs = read_csv('jobs.csv', Job)

    # Use ':memory:' for an in-memory or 'database.db' for a file-backed database
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row

    create_table(conn)
    insert_data(conn, jobseekers, jobs)

    recommendations = generate_recommendation_parallel(conn)

    # Output the recommendations
    print("jobseeker_id, jobseeker_name, job_id, job_title, matching_skill_count, matching_skill_percent")

    for rec in recommendations:
        print(f"{rec['jobseeker_id']}, {rec['jobseeker_name']}, {rec['job_id']}, {rec['job_title']}, {rec['matching_skill_count']}, {rec['matching_skill_percent']:.2f}")


if __name__ == "__main__":
    main()
