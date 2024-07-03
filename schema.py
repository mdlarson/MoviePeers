import graphene
from graphene_sqlalchemy import SQLAlchemyObjectType, SQLAlchemyConnectionField
from models import Actor as ActorModel, Movie as MovieModel, Role as RoleModel


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

    roles = graphene.List(Role, actor_age=graphene.Int())

    def resolve_roles(self, info, actor_age=None):
        session = info.context['session']  # Get session from context
        query = Role.get_query(info)
        print("Resolving roles with actor_age:", actor_age)  # Debug print
        if actor_age is not None:
            print("Query before filter:", query)
            if hasattr(RoleModel, 'actor_age'):
                print("Role model has attribute 'actor_age'")
                query = query.filter(RoleModel.actor_age == actor_age)
            else:
                print("Role model does NOT have attribute 'actor_age'")
            print("Query after filter:", query)

        roles = query.with_session(session).all()
        print(f"Roles found: {roles}")
        return roles


schema = graphene.Schema(query=Query)
