import unittest
import sqlite3
from io import StringIO
from unittest.mock import patch

# Import classes and functions from the main moudle
from job_match_two import JobSeeker, Job, read_csv, create_table, insert_data, match_skills, generate_recommendation_parallel


class TestJobMatching(unittest.TestCase):

    def setUp(self):
        # Setup in-memory SQLite database for testing
        self.conn = sqlite3.connect(':memory:')
        self.conn.row_factory = sqlite3.Row

        # Create tables
        create_table(self.conn)

    def tearDown(self):
        # Close the database connection
        self.conn.close()

    def test_read_csv_jobseeker(self):
        csv_data = """id,name,skills
1,Alice Seeker,Ruby, SQL, Problem Solving
"""
        with patch('builtins.open', return_value=StringIO(csv_data)):
            jobseekers = read_csv('jobseekers.csv', JobSeeker)
            self.assertEqual(len(jobseekers), 1)
            self.assertEqual(jobseekers[0].name, "Alice Seeker")
            self.assertIn("ruby", jobseekers[0].skills)

    def test_read_csv_job(self):
        csv_data = """id,title,required_skills
1,Ruby Developer,Ruby, SQL, Problem Solving
"""
        with patch('builtins.open', return_value=StringIO(csv_data)):
            jobs = read_csv('jobs.csv', Job)
            self.assertEqual(len(jobs), 1)
            self.assertEqual(jobs[0].title, "Ruby Developer")
            self.assertIn("ruby", jobs[0].required_skills)

    def test_insert_data(self):
        jobseekers = [
            JobSeeker(1, "Alice Seeker", "Ruby, SQL, Problem Solving")]
        jobs = [Job(1, "Ruby Developer", "Ruby, SQL, Problem Solving")]
        insert_data(self.conn, jobseekers, jobs)

        cur = self.conn.cursor()
        # Test jobseekers insertion
        cur.execute("SELECT * FROM jobseekers")
        self.assertEqual(len(cur.fetchall()), 1)

        # Test jobs insertion
        cur.execute("SELECT * FROM jobs")
        self.assertEqual(len(cur.fetchall()), 1)

    def test_match_skills(self):
        js_skills = {"ruby", "sql", "problem solving"}
        job_skills = {"ruby", "sql", "problem solving"}
        match_count, match_percent = match_skills(js_skills, job_skills)
        self.assertEqual(match_count, 3)
        self.assertEqual(match_percent, 100)

    def test_generate_recommendations_parralle(self):
        jobseekers = [
            JobSeeker(1, "Alice Seeker", "Ruby, SQL, Problem Solving")]
        jobs = [Job(1, "Ruby Developer", "Ruby, SQL, Problem Solving")]
        insert_data(self.conn, jobseekers, jobs)

        recommendations = generate_recommendation_parallel(self.conn)

        self.assertEqual(len(recommendations), 1)
        self.assertEqual(recommendations[0]['jobseeker_id'], 1)
        self.assertEqual(recommendations[0]['job_id'], 1)
        self.assertEqual(recommendations[0]['matching_skill_percent'], 100.0)


if __name__ == "__main__":
    unittest.main()
