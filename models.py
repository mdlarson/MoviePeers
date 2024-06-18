from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


class Actor(db.Model):
    __tablename__ = 'actors'
    id = db.Column(db.Integer, primary_key=True)
    actor_name = db.Column(db.String, nullable=False)
    birthdate = db.Column(db.String, nullable=False)
    image_path = db.Column(db.String)


class Movie(db.Model):
    __tablename__ = 'movies'
    id = db.Column(db.Integer, primary_key=True)
    movie_title = db.Column(db.String, nullable=False)
    release_date = db.Column(db.String, nullable=False)
    poster_path = db.Column(db.String)


class Role(db.Model):
    __tablename__ = 'roles'
    id = db.Column(db.Integer, primary_key=True)
    actor_id = db.Column(db.Integer, db.ForeignKey('actors.id'))
    movie_id = db.Column(db.Integer, db.ForeignKey('movies.id'))
    actor_age = db.Column(db.Integer)
    actor = db.relationship(
        'Actor', backref=db.backref('roles', lazy='dynamic'))
    movie = db.relationship(
        'Movie', backref=db.backref('roles', lazy='dynamic'))
