from flask import Flask, request, jsonify
from models import db, Employee, Department, Job
from flask_restful import Api, Resource
from sqlalchemy import create_engine, MetaData, Table
import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import errors

from fastavro import writer, reader, parse_schema
import os
from os import environ
import json
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(filename='app.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')

with open('data_dictionary.json') as f:
    DATA_DICTIONARY = json.load(f)

API_BATCH_PROCESSING_LIMIT = 3
# connnection_string = environ.get('DB_URL') TODO DOCKER

connnection_string = 'postgresql://postgres:postgres@localhost:6969/postgres'

# output_dir = environ.get('OUTPUT_FILES') TODO DOCKER

output_dir = '/Users/Chuz/Documents/Flask/first_attempt/output_files/'

historical_files_dir = '/Users/Chuz/Documents/Flask/first_attempt/historical_files/'

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = connnection_string

# Set up the database connection
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db.init_app(app)

# Initialize the API
api = Api(app)


# Create the database tables
with app.app_context():
    db.create_all()
    #db.session.add(Department('9999', 'Admin'))
    db.session.commit()

def validate_request_size(data):
    if len(data) > API_BATCH_PROCESSING_LIMIT:
        result = False
    else:
        result = True
    return result

def valdiate_date_time(dt_str):
    try:
        datetime.fromisoformat(dt_str)
    except:
        return False
    return True


def validate_object(data, data_types, object_name):
    validated_results = []
    bad_objects = []
    result = 'False'

    for item in data:
        object_validation = []
        # This creates an object base
        new_object = globals()[object_name](**item)
        # Make sure the data type is ok 
        for attr, attr_type in data_types.items():
            # Checks individualy attribute by attribute against the data dict
            if type(getattr(new_object, attr)) != attr_type:
                result = False
                print(f'Invalid data type for {attr}')
            else:
                result = True
                print(f'Valid data type for {attr}')
                object_validation.append(result)  
        if all(object_validation):
            validated_results.append(new_object)
        else:
            bad_objects.append(new_object)

    return validated_results, bad_objects


# Define a route to return a 'Hi' message
class Hi():
    @app.route('/hi', methods=['POST'])
    def hi():
        message = 'Hi'
        return jsonify(message)


# Define routes for creating, reading, updating, and deleting employees
class EmployeeResource(Resource):
    @app.route('/employees', methods=['POST'])
    def create_employee():
        """
        Create a new employee.

        Parameters:
        id (int): the employee's ID number
        name (str): the employee's name
        datetime (str): the employee's date of hire
        department_id (int): the ID number of the employee's department
        job_id (int): the ID number of the employee's job

        Returns:
        A JSON object containing the new employee's information.
        """
        data = request.get_json()
        # Define Data Dictionary, change this later on
        # TODO VALIDATE DATE FORMAT 
        data_types = {'id': int, 
                      'name': str, 
                      'datetime': str,  
                      'department_id': str,  
                      'job_id': int
                      }  
        # Get data from request
        data = request.json['data']
        # Check the list has a batch limit
        process_request = validate_request_size(data)
        # Check if the request will be processed
        if process_request:
            # Sets up ojbect name for further valiation
            object_name = "Employee"
            validated_results, bad_objects = validate_object(data, data_types, object_name)
            # TODO LOG BAD OBJECTS
            # Add to the db all rows that are valid according to the data dict rules
            db.session.add_all(validated_results)
            try:
                db.session.commit()
            except:
                db.session.rollback()
                return jsonify({'message': 'An error occurred while creating the Employee.'}), 500

            return jsonify({'message':'User(s) created successfully'}), 201
        else:
            message = f'Validation failed. Please check your input keep the items \
                limit to {API_BATCH_PROCESSING_LIMIT} items.'
            return jsonify({'message':str(message)}), 500

    @app.route('/employees/<int:id>', methods=['GET'])
    def get_employee(id):
        """
        Get an employee's information.

        Parameters:
        id (int): the employee's ID number

        Returns:
        A JSON object containing the employee's information.
        """
        try:
            employee = Employee.query.get(id)
            return jsonify(employee.as_dict())
        except Exception as e:
            return jsonify({'error': str(e)})
        finally:
            db.session.close()

    @app.route('/employees/<int:id>', methods=['PUT'])
    def update_employee(id):
        """
        Update an employee's information.

        Parameters:
        id (int): the employee's ID number
        name (str): the employee's name
        datetime (str): the employee's date of hire
        department_id (int): the ID number of the employee's department
        job_id (int): the ID number of the employee's job

        Returns:
        A JSON object containing the updated employee's information.
        """
        data = request.get_json()
        try:
            employee = Employee.query.get(id)
            employee.name = data['name']
            employee.datetime = datetime.strptime(data['datetime'], "%Y-%m-%dT%H:%M:%SZ")  
            employee.department_id = data['department_id']
            employee.job_id = data['job_id']
            db.session.commit()
            return jsonify(employee.as_dict())
        except Exception as e:
            db.session.rollback()
            return jsonify({'error': str(e)})
        finally:
            db.session.close()

    @app.route('/employees/<int:id>', methods=['DELETE'])
    def delete_employee(id):
        """
        Delete an employee.

        Parameters:
        id (int): the employee's ID number

        Returns:
        A message indicating that the employee was deleted.
        """
        try:
            employee = Employee.query.get(id)
            db.session.delete(employee)
            db.session.commit()
            return jsonify({'message': 'Employee deleted.'})
        except Exception as e:
            db.session.rollback()
            return jsonify({'error': str(e)})
        finally:
            db.session.close()

    @app.route('/employees/load', methods=['POST'])
    def load_employees():

        try:
            if str(request.json['drop_all']) == "True" :
            # Drop all data from the Job table
                db.session.query(Employee).delete()
                db.session.commit()


            file_name = historical_files_dir + request.json['file_name']
            # Read the CSV file using pandas
            df = pd.read_csv(file_name, header=None, names=['id','name', 'datetime','department_id','job_id'], 
                             dtype=str, na_values=['', 'NaN', 'N/A', 'na'])
            # Records with incomplete data will need to replace nan with None to work well with db logic
            df = df.replace({np.nan: None})

            for index, row in df.iterrows():
                try:
                    employee = Employee(id=row['id'], 
                                        name=row['name'],  
                                        datetime = datetime.strptime(row['datetime'], "%Y-%m-%dT%H:%M:%SZ") , 
                                        department_id=row['department_id'], 
                                        job_id=row['job_id'])
                    db.session.add(employee)
                    db.session.commit()

                except (ValueError, TypeError) as e:
                # Log the bad row to a file
                    logging.error(f"Bad row {index}: {row}. Error: {str(e)}")
                    db.session.rollback()
                    continue
                except psycopg2.Error as e:
                    logging.error(f"Bad row {index}: {row}. Error: {str(e)}")
                    db.session.rollback()
                    continue

            return jsonify({'message': 'Data loaded successfully'}), 201
        except Exception as e:
            logging.error({'error': str(e)})

            return jsonify({'error': str(e)}), 500
        

# Define endpoints for Department resource
class DepartmentResource(Resource):
    """
    DepartmentResource is a Flask-RESTful Resource that defines the endpoints
    for creating, reading, updating, and deleting departments.
    """

    @app.route('/departments', methods=['POST'])
    def create_department():
        """
        Creates a new department.

        Parameters:
        - `id`: the department's ID
        - `department`: the department's name
        """
        # Define Data Dictionary, change this later on
        data_types = {'id': int, 
                      'department': str}  
        # Get data from request
        data = request.json['data']
        # Check the list has a batch limit
        process_request = validate_request_size(data)
        
        # Check if the request will be processed

        # Check if the request will be processed
        if process_request:
            # Sets up ojbect name for further valiation
            object_name = "Department"
            validated_results, bad_objects = validate_object(data, data_types, object_name)
            # TODO LOG BAD OBJECTS
            # Add to the db all rows that are valid according to the data dict rules
            db.session.add_all(validated_results)
            try:
                db.session.commit()
            except:
                db.session.rollback()
                return jsonify({'message': 'An error occurred while creating the Department.'}), 500
            message = 'User(s) created successfully'
            return jsonify({'message': str(message)}), 201
        else:
            message = f'Validation failed. Please check your input keep the items \
                limit to {API_BATCH_PROCESSING_LIMIT} items.'
            return jsonify({'message': str(message)}), 500


    @app.route('/departments/<int:id>', methods=['GET'])
    def get_department(id):
        """
        Retrieves a department by id.

        Parameters:
        - `id`: the department's ID
        """
        department = Department.query.get(id)
        if department is None:
            return jsonify({'message': 'Department not found.'}), 404
        return jsonify(department.as_dict())

    @app.route('/departments/<int:id>', methods=['PUT'])
    def update_department(id):
        """
        Updates a department by id.

        Parameters:
        - `id`: the department's ID
        - `department`: the department's name
        """
        data = request.get_json()
        department = Department.query.get(id)
        if department is None:
            return jsonify({'message': 'Department not found.'}), 404
        department.department = data['department']
        try:
            db.session.commit()
        except:
            db.session.rollback()
            return jsonify({'message': 'An error occurred while updating the department.'}), 500
        return jsonify(department.as_dict())

    @app.route('/departments/<int:id>', methods=['DELETE'])
    def delete_department(id):
        """
        Deletes a department by id.

        Parameters:
        - `id`: the department's ID
        """
        department = Department.query.get(id)
        if department is None:
            return jsonify({'message': 'Department not found.'}), 404
        try:
            db.session.delete(department)
            db.session.commit()
        except:
            db.session.rollback()
            return jsonify({'message': 'An error occurred while deleting the department.'}), 500
        return jsonify(department.as_dict())
    

    @app.route('/departments/load', methods=['POST'])
    def load_departments():

        try:

            if str(request.json['drop_all']) == "True" :
            # Drop all data from the Job table
                db.session.query(Department).delete()
                db.session.commit()

            file_name = historical_files_dir + request.json['file_name']

            # Read the CSV file using pandas
            df = pd.read_csv(file_name, header=None, names=['id','department'], dtype=str)

            # Map the DataFrame rows to Department objects and insert them into the database
            for index, row in df.iterrows():
                department = Department(id=row['id'], department=row['department'])
                db.session.add(department)
            db.session.commit()
            return jsonify({'message': 'Data loaded successfully'}), 201
        except Exception as e:
            return jsonify({'error': str(e)}), 500


# Define endpoints for Job resource
class JobResource(Resource):

    @app.route('/jobs', methods=['POST'])
    def create_user():
        # Define Data Dictionary, change this later on
        data_types = {'id': int, 
                      'job': str}  
        # Get data from request
        data = request.json['data']
        # Check the list has a batch limit
        process_request = validate_request_size(data)
        # Check if the request will be processed
        if process_request:
            # Sets up ojbect name for further valiation
            object_name = "Job"
            validated_results, bad_objects = validate_object(data, data_types, object_name)
            # TODO LOG BAD OBJECTS
            # Add to the db all rows that are valid according to the data dict rules
            db.session.add_all(validated_results)
            try:
                db.session.commit()
            except:
                db.session.rollback()
                return jsonify({'message': 'An error occurred while creating the Job.'}), 500
            message = 'User(s) created successfully'
            return jsonify({'message': str(message)}), 201
        else:
            message = f'Validation failed. Please check your input keep the items \
                limit to {API_BATCH_PROCESSING_LIMIT} items.'
            return jsonify({'message': str(message)}), 500
    
    @app.route('/jobs', methods=['GET'])
    def get_job(id):
        """
        Retrieves the job with the specified id from the database.

        :param id: id of the job to retrieve
        :return: JSON object representing the job
        """
        job = Job.query.get(id)
        if job is None:
            return jsonify({'message': 'Job not found.'}), 404
        return jsonify(job.as_dict())

    @app.route('/jobs', methods=['PUT'])
    def update_job(id):
        """
        Updates the job with the specified id in the database.

        :param id: id of the job to update
        :return: JSON object representing the updated job
        """
        data = request.get_json()
        job = Job.query.get(id)
        if job is None:
            return jsonify({'message': 'Job not found.'}), 404
        job.job = data['job']
        try:
            db.session.commit()
        except:
            db.session.rollback()
            return jsonify({'message': 'An error occurred while updating the job.'}), 500
        return jsonify(job.as_dict())

    @app.route('/jobs', methods=['DELETE'])
    def delete_job(id):
        """
        Deletes the job with the specified id from the database.

        :param id: id of the job to delete
        :return: JSON object representing the deleted job
        """
        job = Job.query.get(id)
        if job is None:
            return jsonify({'message': 'Job not found.'}), 404
        try:
            db.session.delete(job)
            db.session.commit()
        except:
            db.session.rollback()
            return jsonify({'message': 'An error occurred while deleting the job.'}), 500
        return jsonify(job.as_dict())
    
    @app.route('/jobs/drop-all', methods=['DELETE'])
    def drop_all_jobs():
        try:
            db.session.query(Job).delete()
            db.session.commit()
            return jsonify({'message': 'All jobs dropped successfully'}), 200
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    

    @app.route('/jobs/load', methods=['POST'])
    def load_jobs():

        try:
            if str(request.json['drop_all']) == "True" :
            # Drop all data from the Job table
                db.session.query(Job).delete()
                db.session.commit()
                    
            file_name = historical_files_dir + request.json['file_name']
        
            # Read the CSV file using pandas
            df = pd.read_csv(file_name, header=None, names=['id','job'])

            # Map the DataFrame rows to Job objects and insert them into the database
            for index, row in df.iterrows():
                job = Job(id=row['id'], job=row['job'])
                db.session.add(job)
            db.session.commit()
            return jsonify({'message': 'Data loaded successfully'}), 201
        except Exception as e:
            return jsonify({'error': str(e)}), 500


class DataBaseUtilities(Resource):

    @app.route('/sample_query', methods=['GET'])
    def sample_query():
        query = db.session.query(
            Department.department,
            Job.job,
            db.func.count(Employee.id)
        ).join(Employee, Department.id == Employee.department_id)\
        .join(Job, Employee.job_id == Job.id)\
        .group_by(Department.department, Job.job)
        
        results = query.all()
        response = [{'department': r[0], 'job': r[1], 'count': r[2]} for r in results]
        return jsonify(response)




    @app.route('/get_csv_db2', methods=['POST'])
    def something_else():
        # Get data from request
        data = request.json['data']

        for item in data:
            # Connect to the database using SQLAlchemy
            engine = create_engine(connnection_string)

            # Load the data from the database into a Pandas dataframe
            df = pd.read_sql_table(item['table'], engine)

            # Write the data to a CSV file
            #df.to_csv('./app/output_files/' + item['table'] + '_table_backup.csv', index=False)

            file_name = output_dir + item['table'] + '_table_backup.csv'

            df.to_csv(file_name, index=False)
            print(file_name)
            logging.info("A Debug Logging Message %1")
            logging.info("Some message: a=%s ", file_name)
            log = logging.getLogger(__name__)
            log.info("An unexpected error occurred!", exc_info=True)

        return {'message': 'CSV from db file created successfully'}

    @app.route('/load_employees', methods=['POST'])
    def get_csv_db():
        try:
            # Read the CSV file using pandas
            df = pd.read_csv('data/employees.csv')
            # Map the DataFrame rows to Employee objects and insert them into the database
            for index, row in df.iterrows():
                employee = Employee(id=row['id'], name=row['name'], datetime=row['datetime'], 
                                    department_id=row['department_id'], job_id=row['job_id'],dtype=str)
                db.session.add(employee)
            db.session.commit()
            return 'Data loaded successfully', 201
        except Exception as e:
            return str(e), 500



    
    @app.route('/avro', methods=['POST'])
    def avro():
        # Get data from request
        data = request.json['data']

        for table in data:
            # Connect to the database using SQLAlchemy
            engine = create_engine(connnection_string)

            # Load the data from the database into a Pandas dataframe
            df = pd.read_sql_table(table, engine)

            # Define the Avro schema for the table
            schema = fastavro.schema.parse_schema({
                "type": "record",
                "name": str(table),
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "department", "type": "string"},
                ]
            })

            parsed_schema = parse_schema(schema)

            # 2. Convert pd.DataFrame to records - list of dictionaries
            records = df.to_dict('records')

            # 3. Write to Avro file
            with open(output_dir +'prices.avro', 'wb') as out:
                writer(out, parsed_schema, records)

        return {'message': 'Avro file created successfully'}




api.add_resource(DataBaseUtilities, '/avro')
api.add_resource(EmployeeResource, '/employees', '/employees/<int:id>')
api.add_resource(DepartmentResource, '/departments', '/departments/<int:id>')
api.add_resource(JobResource, '/jobs', '/jobs/<int:id>')


if __name__ == '__main__':
    app.run(debug=True)