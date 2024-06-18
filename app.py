from flask import Flask, render_template, request
from flask_graphql import GraphQLView
from models import db
from schema import schema

# Initialize Flask app
app = Flask(__name__,
            template_folder='./templates',
            static_folder='./static')

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///actors_movies.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)

app.add_url_rule(
    '/graphql',
    view_func=GraphQLView.as_view(
        'graphql',
        schema=schema,
        graphiql=True  # Enable GraphiQL interface
    )
)


# Define app routes
@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        age = int(request.form['age'])
        # use GraphQL to fetch data
        query = f'''
        {{
            allRoles(filter: {{actorAge: {age}}}) {{
                edges {{
                    node {{
                        actor {{
                            actorName
                        }}
                        movie {{
                            movieTitle
                        }}
                    }}
                }}
            }}
        }}
        '''
        result = schema.execute(query)

        # Debug result
        print('GraphQL Query Results: ', result)

        if result.errors:
            return render_template('index.html', message="An error occurred. :(" + str(result.errors), result=None)

        data = result.data.get('allRoles') if result.data else None

        if data and data['edges']:
            node = data['edges'][0]['node']
            message = f"You're about as old as {
                node['actor']['actorName']} in {node['movie']['movieTitle']}."
        else:
            message = "Sorry, we couldn't find a good match."

        return render_template('index.html', message=message, result=data)
    return render_template('index.html', message='', result=None)


if __name__ == '__main__':
    app.run(debug=True)
