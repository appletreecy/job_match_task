# Job Matching Application

This project is a job matching application that reads jobseekers and job listing from CSV files, stores them in an SQLite database, and generates job recommendations for each jobseeker based on matching skills.

## Table of Contents

- [Job Matching Application](#job-matching-application)
  - [Table of Contents](#table-of-contents)
  - [Features](#features)
  - [Setup](#setup)
  - [Usage](#usage)
  - [Testing](#testing)

## Features

- Reads jobseekers and job listings from CSV files
- Store the data in an SQLite database
- Generates job recommendation based on matching skills
- Outputs recommendatioins sorted by jobseeker ID and the percentage of matching skills.

## Setup

1. **Clone the Repository**
   '''

   '''

Usage

1. Prepare CSV files
   `jobseekers.csv`: Contains jobseeker information
   `jobs.csv`: Contains job listings.

2. Run the Application

   python job_match_two.py

   This will generate job recommedations and output them to the console.

Testing

1. Run Unit Tests
   Ensure that your `test_main_module.py` file is in the same directory as your application code and run

   python -m unittest test_main_module.py

   The tests will verify the functionality of reading CSV files, inserting data into the database, matching skills and generating recommendations.
