from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

class Employee(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120))
    datetime = db.Column(db.DateTime)
    department_id = db.Column(db.Integer, db.ForeignKey('department.id'), nullable=True)
    job_id = db.Column(db.Integer, db.ForeignKey('job.id'), nullable=True)

    def __init__(self, id, name, datetime, department_id, job_id):
        self.id = id
        self.name = name
        self.datetime = datetime
        self.department_id = department_id
        self.job_id = job_id

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def data_type_as_dict(self):
        return {col.name: str(col.type) for col in Job.__table__.columns}   

class Department(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    department = db.Column(db.String(120))

    def __init__(self, id, department):
        self.id = id
        self.department = department
 
    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
    
    def data_type_as_dict(self):
        return {col.name: str(col.type) for col in Job.__table__.columns}

class Job(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    job = db.Column(db.String(120))

    def __init__(self, id, job):
        self.id = id
        self.job = job
 
    def as_dict(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}
    
    def data_type_as_dict(self):
        return {col.name: str(col.type) for col in Job.__table__.columns}