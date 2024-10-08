import os
import random
from flask import Flask, render_template, request
from flask_graphql import GraphQLView
from models import db
from schema import schema

# Initialize Flask app
app = Flask(__name__,
            template_folder='./templates',
            static_folder='./static',
            instance_relative_config=True)

# Ensure the correct database URI is set
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv(
    'DATABASE_URL', f"sqlite:///{os.path.join(app.instance_path, 'moviedata.db')}")
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ECHO'] = False  # Suppress SQL logs

# Initialize the database with the app
db.init_app(app)

with app.app_context():
    # Ensure tables are created before the first request
    db.create_all()

# Add GraphQL endpoint
app.add_url_rule(
    '/graphql',
    view_func=GraphQLView.as_view(
        'graphql',
        schema=schema,
        graphiql=True  # Enable GraphiQL interface
    )
)


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        age = int(request.form['age'])
        # Use GraphQL query to fetch data
        query = f'''
        {{
            roles(actorAge: {age}) {{
                actor {{
                    actorName
                    imagePath
                }}
                movie {{
                    movieTitle
                    posterPath
                }}
            }}
        }}
        '''
        result = None
        with app.app_context():
            result = schema.execute(query, context_value={
                                    'session': db.session})
        # Debug result
        # print('GraphQL Query Results: ', result)

        if result.errors:
            return render_template('index.html', message="An error occurred. :(" + str(result.errors), result=None)

        data = result.data.get('roles') if result.data else None

        if data and len(data) > 0:
            # Randomly select a role from the list of results
            node = random.choice(data)
            message = f"You're about as old as...<br>\
            {node['actor']['actorName']} in <i>{node['movie']['movieTitle']}</i>."
            return render_template('index.html', message=message, result=node, age=age)

        else:
            message = "Sorry, we couldn't find a good match."
            return render_template('index.html', message=message, result=None, age=age)

    return render_template('index.html', message='', result=None, age=None)


if __name__ == '__main__':
    app.run()
