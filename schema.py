import graphene
from graphene_sqlalchemy import SQLAlchemyObjectType, SQLAlchemyConnectionField
from models import Actor as ActorModel, Movie as MovieModel, Role as RoleModel
from models import db


class Actor(SQLAlchemyObjectType):
    class Meta:
        model = ActorModel
        interfaces = (graphene.relay.Node,)


class Movie(SQLAlchemyObjectType):
    class Meta:
        model = MovieModel
        interfaces = (graphene.relay.Node,)


class Role(SQLAlchemyObjectType):
    class Meta:
        model = RoleModel
        interfaces = (graphene.relay.Node,)


class Query(graphene.ObjectType):
    node = graphene.relay.Node.Field()
    all_actors = SQLAlchemyConnectionField(Actor.connection)
    all_movies = SQLAlchemyConnectionField(Movie.connection)
    all_roles = SQLAlchemyConnectionField(Role.connection)

    role_by_age = graphene.Field(Role, age=graphene.Int())

    def resolve_role_by_age(self, info, age):
        return RoleModel.query.filter_by(actor_age=age).first()


schema = graphene.Schema(query=Query)
