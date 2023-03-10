from base64 import encode
from flask import Flask, Request,json, Response ,jsonify, redirect, render_template
from neo4j import GraphDatabase
import csv
with open("cred.txt") as file:
    data = csv.reader(file, delimiter=',')
    for row in data:
        username = row[0]
        pwd = row[1]
        uri = row[2]
#print(username, pwd, uri)

driver = GraphDatabase.driver(uri=uri, auth=(username, pwd))
session = driver.session()
api = Flask(__name__)
@api.route("/create/<string:name>&<int:id>", methods=["GET", "POST"])
def create_node(name, id):
    query = """
    CREATE (n:Employee {Name:$name, ID:$id})
    """
    map = {"name":name, "id":id}
    try:
        session.run(query, map)
        return f"Created node with name = {name} amd id = {id}"
    except Exception as e:
        return (str(e))

@api.route("/display/<string:name>", methods=["GET", "POST"])
def display_name(name):
    query = """
    MATCH (p:personOne|personTwo {name: $name})
    RETURN p
    """
    map = {"name":name}
    try:
        results = session.run(query, map)
        data = results.data()
        json_string = json.dumps(data, ensure_ascii=False)
        response = Response(json_string, content_type="application/json; charset=utf-8")
        return response
    except Exception as e:
        return (str(e))

if __name__ == "__main__":
    api.run(port = 5050)