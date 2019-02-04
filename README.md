# Assignment-13

import numpy as np
import pandas as pd
import pandasql as sql

datasource=("https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data")
col_list = ['age','workclass','fnlwgt','education','education-num','marital-status','occupation',
           'relationship','race','sex','capital-gain','capital-loss','hours-per-week','native-country','Label']
# Import the data from the url above into Pandas DataFrame
adultdata = pd.read_csv(datasource,sep=",",delimiter=",",names=col_list,skipinitialspace=True)

#Printing the existing list of column headers
print(adultdata.columns)

#Removing "-" from the column headers as the is not a valid character in a database. We will replace it with "_"
import re
#using for loop to capture all '-' and replace it with '_'
adultdata.columns = [re.sub("[-]", "_", col) for col in adultdata.columns]
#Prinitng revised list of columns
print(adultdata.columns)

adultdata.head()

#1.Create an sqlalchemy engine using a sample from the data set

#Importing all relevant libraries

from sqlalchemy import create_engine, ForeignKey
from sqlalchemy import Column , Integer , String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref

from sqlalchemy import create_engine
#Creating the database engine
engine = create_engine('sqlite:///:memory:', echo=True)

from sqlalchemy import create_engine, ForeignKey
from sqlalchemy import Column, Date, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref

#Connecting the adultdata dataframe to sql database
adultdata.to_sql('adultdata', con=engine)

from sqlalchemy.orm import Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Creating an Object to hold SQLAlchemy data types to map properties of Python classes into columns on a 
# relation database table
Base = declarative_base(engine)
class adultdata(Base):
    __tablename__ = 'adultdata'
    __table_args__ = {'autoload': True}
    
    index = Column(Integer(), primary_key=True)
    age = Column(Integer())
    workclass = Column(String())

#Create SQLAlchemy sessions

def loadSession():
    """"""
    metadata = Base.metadata
    Session = sessionmaker(bind=engine)
    session = Session()
    return session
    
    #Querying Data with SQLAlchemy ORM
if __name__ == "__main__":
    session = loadSession()
    rows = session.query(adultdata).first()
    print("AGE","SEX   ","WORKCLAS ","COUNTRY ","OCCUPATION")
    print(rows.age,rows.sex,rows.workclass,rows.native_country,rows.occupation)
    
    # 2. Write two basic update queries
    print('Update the workclass and occupation to Private and Data Scientist respectively where fnlwgt = 77516')
print("")
#Update Data with SQLAlchemy ORM
if __name__ == "__main__":
    session = loadSession()
    rows = session.query(adultdata).filter_by(fnlwgt=77516).first()
    print(rows)
    rows.occupation ='Data Scientist'
    rows.workclass = 'Private'
    session.commit()
    
    #verify update results

if __name__ == "__main__":
    session = loadSession()
    rows = session.query(adultdata).filter_by(fnlwgt=77516).first()
    print("occupation : ",rows.occupation,"workclass :",rows.workclass,"fnlwgt : ",rows.fnlwgt)
    
   #update query 2 

print("Update occupation of every person who has '11th' as education and age =53 as Fireman")
print(" ")

if __name__ == "__main__":
    session = loadSession()
    row = session.query(adultdata).filter_by(age=53,education='11th').all()
    for i in row:
        i.occupation = 'Fireman'
    session.commit()
    
    #verify results after update

if __name__ == "__main__":
    session = loadSession()
    row = session.query(adultdata).filter_by(age=53,education='11th').all()
    for i in row:
        print(i.age,i.education,i.sex,i.occupation,i.workclass)
        
        # 3. Write two delete queries

#Check for all rows which have occupation as ? - Querying Data with SQLAlchemy ORM
if __name__ == "__main__":
    session = loadSession()
    rows = session.query(adultdata).filter_by(occupation="?").all()
    print("Count of rows before delete operation : ",len(rows))
    
    #Delete rows which have occupation as "?" - Delete Data with SQLAlchemy ORM

if __name__ == "__main__":
    session = loadSession()
    session.query(adultdata).filter_by(occupation="?").delete(synchronize_session='fetch')
    session.commit()
    rows = session.query(adultdata).filter_by(occupation="?").all()
    print("Count of rows after delete operation : ",len(rows))
    
    #Delete 2
#Check for all rows which have education as  Some-college- Querying Data with SQLAlchemy ORM
if __name__ == "__main__":
    session = loadSession()
    rows = session.query(adultdata).filter_by(education="Some-college").all()
    print("count of rows :",len(rows))
    
    #delete rows from table adultdb which have education as "Some-college" - Delete Data with SQLAlchemy ORM
#delete(synchronize_session='fetch')
#'fetch' - performs a select query before the delete to find objects that are matched by the delete query and 
#need to be removed from the session. Matched objects are removed from the session
if __name__ == "__main__":
    session = loadSession()
    session.query(adultdata).filter_by(education="Some-college").delete(synchronize_session='fetch')
    session.commit()
    rows = session.query(adultdata).filter_by(education="Some-college").all()
    print("Count of rows after Delete : ",len(rows))
    
    # 4. Write two filter queries

# Querying Data with SQLAlchemy ORM based on filter on workclass column having a value as "Private"
if __name__ == "__main__":
    session = loadSession()
    rows = session.query(adultdata).filter_by(workclass='Private').all()
    for i in rows:
        print(i.age,i.sex,i.workclass,i.native_country)
        
        # Querying Data with SQLAlchemy ORM based on filter on marital_status column having a value as "Never-married"
count = 0
for rows in engine.execute("SELECT * FROM adultdata where marital_status == 'Never-married'").fetchall():
    print(rows.age,",",rows.marital_status,",",rows.occupation)
    count+=1
print("Total Number of rows fetched : ",count)

# 5. Write two function queries

# Querying Data with SQLAlchemy ORM - Use aggregate Function -count the number of male and female based on sex 
#data column values
# import func module from sqlalchemy.sql package in order to perform aggregate functions
from sqlalchemy.sql import func
if __name__ == "__main__":
    session = loadSession()
    result = session.query(func.count(adultdata.age).label('count_age'), adultdata.sex ).group_by(adultdata.sex).all()
    print("Count of people based on sex ")
    print("")          
    for rows in result:
        print(rows)
        
        # Querying Data with SQLAlchemy ORM - Use aggregate Function -average age and minimum age of people based on sex 
#column value .
if __name__ == "__main__":
    session = loadSession()
    result = session.query(func.avg(adultdata.age).label('avg_age'),func.min(adultdata.age).label('min_age'), 
                           adultdata.sex).group_by(adultdata.sex).all()
    print("Average age,minimum age  of people based on sex ")
    print("")
    for rows in result:
        print(rows)
