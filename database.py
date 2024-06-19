from flask import Flask
from models import db, Actor, Movie, Role
# from sqlalchemy import inspect

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///actors_movies.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)

with app.app_context():
    db.create_all()

    actor1 = Actor(actor_name='Actor One', birthdate='1990-01-01',
                   image_path='/static/images/actor_one.jpg')
    actor2 = Actor(actor_name='Actor Two', birthdate='1985-05-05',
                   image_path='/static/images/actor_two.jpg')

    movie1 = Movie(movie_title='Movie One', release_date='2010-07-16',
                   poster_path='/static/images/movie_one.jpg')
    movie2 = Movie(movie_title='Movie Two', release_date='2005-11-18',
                   poster_path='/static/images/movie_two.jpg')

    role1 = Role(actor=actor1, movie=movie1, actor_age=20)
    role2 = Role(actor=actor2, movie=movie2, actor_age=30)

    db.session.add_all([actor1, actor2, movie1, movie2, role1, role2])
    db.session.commit()

    # Use SQLAlchemy inspector to get detailed information about the 'roles' table
    # inspector = inspect(db.engine)
    # columns = inspector.get_columns('roles')
    # for column in columns:
    #     print("Column: {} Type: {}".format(column['name'], column['type']))
