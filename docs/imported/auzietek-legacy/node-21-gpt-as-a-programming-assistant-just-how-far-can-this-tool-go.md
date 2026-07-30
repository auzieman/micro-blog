---
title: "GPT as a programming assistant? Just how far can this tool go?"
slug: "gpt-as-a-programming-assistant-just-how-far-can-this-tool-go"
summary: "Legacy Auzietek capture staged for cleanup, tagging, and lane assignment."
status: draft
source_url: "https://auzietek.com/node/21"
source_id: "node-21"
captured_at: "2026-07-29"
candidate_lane: unreviewed
tags: [legacy, auzietek, needs-review]
assets:
  []
---

Asking it what it thought of a rough idea I'm sure many programmers have thought about, a custom mini Ansible..

This isn't even done, its workable but just an idea..

```
#!/usr/bin/env python3
from fabric import *
from jinja2 import *
import json
import os
import requests

rules=json.load(open('rules.json', 'r'))
path=os.getcwd()

#do this per group
dictionary_merge=something
jinja=something

for group in rules["groups"]:
    group
    for host in rules["groups"][group]["nodes"]:
        final_dict={**rules["globals"],**rules["groups"][group]["locals"], **rules["groups"][group]["nodes"][host]}
        file_loader = FileSystemLoader('templates')
        env = Environment(loader=file_loader)
        template = env.get_template('deploy.sh.j2')
        output = template.render(final_dict)
        print(output)
        filename="deploy_"+str(group)+"_"+str(host)+".sh"
        with open(str(os.path.join(path,filename)), "w") as f:
            f.write(output)
        user=rules["groups"][group]["nodes"][host]["user"]
        con= Connection(host=host, user=user)
        //scp files as user host
        con.put(str(os.path.join(path,filename)), '/var/tmp/')
        //ssh exec files as user host
        cmd="/bin/cat /var/tmp/"+str(filename)
        con.sudo(cmd)

json_data = None
with open('example.json') as json_file:
    json_data = json.load(json_file)

auth=('token', 'example')

r = requests.post('https://api.example.com/api/dir/v1/accounts/9999999/orders', json=json_data, auth=auth)
```

GPT:

The code you posted is a Python script that uses the Fabric and Jinja2 libraries to generate shell scripts from a JSON file. The script first loads the "rules.json" file and creates a dictionary from the data. It then loops over the "groups" in the dictionary, and for each group, it loops over the "nodes" in the group. For each node, the script creates a dictionary of key-value pairs from the various dictionaries in the rules.json file and uses the Jinja2 library to render a shell script template with the data. The script then writes the output to a file with a unique name for each node and transfers the file to the node using SCP, then runs the shell script on the node using SSH.

The script then opens another JSON file, "example.json", and uses the Requests library to send a POST request to an API with the JSON data and some authentication information.

If you're looking to add a web UI, you can use a web framework such as Flask or Django to create the UI and allow users to interact with the code. For example, you can create a form in the web UI that takes in the required parameters, then sends a request to the backend to generate the shell scripts and run them on the nodes. The backend can return the results to the UI, which can display them to the user.

ME:

Then I inadvertently got it to consider the json file,

Yes the tricky part is that rules.json file I would want a form that dynamically expresses editable objects I can show you an example I called rulesb.json.. You absolutely nailed translating the main loop btw wow..

GPT:

Here's an example of what the "rulesb.json" file could look like:

```
{
"globals": {
"global_var_1": "value_1",
"global_var_2": "value_2"
},
"groups": {
"group_1": {
"locals": {
"group_var_1": "group_value_1",
"group_var_2": "group_value_2"
},
"nodes": {
"host_1": {
"user": "user_1",
"node_var_1": "node_value_1",
"node_var_2": "node_value_2"
},
"host_2": {
"user": "user_2",
"node_var_1": "node_value_3",
"node_var_2": "node_value_4"
}
}
},
"group_2": {
"locals": {
"group_var_1": "group_value_3",
"group_var_2": "group_value_4"
},
"nodes": {
"host_3": {
"user": "user_3",
"node_var_1": "node_value_5",
"node_var_2": "node_value_6"
},
"host_4": {
"user": "user_4",
"node_var_1": "node_value_7",
"node_var_2": "node_value_8"
}
}
}
}
}
```

This is just an example and the actual content of the file will depend on your specific requirements. The structure is similar to the "rules.json" file, with a top-level dictionary that contains two keys, "globals" and "groups". The "globals" key is a dictionary of global variables, and the "groups" key is a dictionary of groups, where each group is a dictionary with "locals" and "nodes" keys. The "locals" key is a dictionary of variables specific to that group, and the "nodes" key is a dictionary of nodes, where each node is a dictionary with various key-value pairs for that node.

ME:

Exactly and wow you got the structure almost perfectly maybe even better than my test structure, very impressive. What makes this form a challenge is it should be like bubbles or legos ie each object type, array element is a little differnt but all fits together, a person should be able to add a new element or edit an existing one and maybe move node1 to another group etc or copy a node to other groups IE one stage edits the database the other a webservice so data first ui second same node in two groups.

It sounds like you're looking for a dynamic and flexible form for editing the rules.json file, with the ability to add and edit elements and move nodes between groups. To achieve this, you could consider using a web framework like Flask or Django, which provides a way to create dynamic web pages and can interact with the backend data stored in the rules.json file.

GPT:

In this setup, you could have a web form that allows users to edit the data stored in the rules.json file, and a backend API that updates the file and runs the script to generate the shell scripts based on the updated data. This way, you can ensure that the changes made in the form are reflected in the output files and the scripts run with the updated data.

If you'd like, I could help you implement this by writing some code snippets in the language and framework of your choice.

ME:

Flask would be nice and you really seem to get what I'm thinking so sure blow my mind some more :)

GPT:

Sure! I can help you build a dynamic form using Flask to edit the rules.json file. Flask is a lightweight web framework that allows you to easily build web applications in Python. To create a form using Flask, you can start by defining the structure of your form as a Python dictionary. For example:

python code:

```
form = {
    "group": {
        "type": "string",
        "label": "Group Name",
        "required": True
    },
    "nodes": {
        "type": "list",
        "label": "Nodes",
        "required": True,
        "children": {
            "host": {
                "type": "string",
                "label": "Host",
                "required": True
            },
            "user": {
                "type": "string",
                "label": "User",
                "required": True
            }
        }
    }
}
```

Next, you can use the form definition to dynamically generate an HTML form in your Flask application. For example:

Template file.

```
<form action="/submit" method="post">
    {% for field in form %}
        <div>
            <label for="{{ field }}">{{ form[field]["label"] }}:</label>
            {% if form[field]["type"] == "string" %}
                <input type="text" name="{{ field }}" required="{{ form[field]["required"] }}">
            {% elif form[field]["type"] == "list" %}
                <div>
                    <h3>{{ form[field]["label"] }}:</h3>
                    {% for child in form[field]["children"] %}
                        <div>
                            <label for="{{ child }}">{{ form[field]["children"][child]["label"] }}:</label>
                            <input type="text" name="{{ child }}" required="{{ form[field]["children"][child]["required"] }}">
                        </div>
                    {% endfor %}
                </div>
            {% endif %}
        </div>
    {% endfor %}
    <input type="submit" value="Submit">
</form>
```

Finally, you can handle the form submission in your Flask application to update the rules.json file with the new data. For example:

python code:

```
@app.route('/submit', methods=['POST'])
def submit():
    data = request.form
    # update rules.json with the new data
    with open('rules.json', 'w') as f:
        json.dump(data, f)
    return redirect('/')
```

This is just a simple example, but it should give you an idea of how to build a dynamic form using Flask. You can easily customize and extend this example to fit your specific needs.

ME:

could you refactor my core script?

GPT

Sure, I can help you refactor the code. Here's a refactored version of your code in Python that uses Flask to create a web UI for your script:python

Copy code

```
from fabric import Connection
from jinja2 import Environment, FileSystemLoader
import json
import os
import requests
from flask import Flask, request, render_template

app = Flask(__name__)

def generate_scripts(rules):
    path = os.getcwd()
    file_loader = FileSystemLoader('templates')
    env = Environment(loader=file_loader)

    for group in rules["groups"]:
        for host in rules["groups"][group]["nodes"]:
            final_dict = {**rules["globals"], **rules["groups"][group]["locals"], **rules["groups"][group]["nodes"][host]}
            template = env.get_template('deploy.sh.j2')
            output = template.render(final_dict)
            filename = "deploy_{}_{}.sh".format(group, host)
            with open(os.path.join(path, filename), "w") as f:
                f.write(output)
            user = rules["groups"][group]["nodes"][host]["user"]
            con = Connection(host=host, user=user)
            con.put(os.path.join(path, filename), '/var/tmp/')
            cmd = "/bin/cat /var/tmp/{}".format(filename)
            con.sudo(cmd)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        rules = request.form.to_dict()
        generate_scripts(rules)
        json_data = None
        with open('example.json') as json_file:
            json_data = json.load(json_file)
        auth = ('token', 'example')
        r = requests.post('https://api.example.com/api/dir/v1/accounts/9999999/orders', json=json_data, auth=auth)
        return render_template('index.html', message='Scripts generated successfully!')
    return render_template('index.html')

if __name__ == '__main__':
    app.run()
```

This code uses Flask to create a web application that allows users to generate scripts by submitting form data through the web UI. The form data is used to update the rules dictionary, which is then passed to the generate_scripts function to generate the scripts. Finally, the requests.post method is used to send the JSON data to the specified API endpoint.

You'll also need to create a templates folder that contains the Jinja templates for your scripts, and an index.html template for your web UI.

Summary,

I may actually make this into a project, its a toss up to be terrified or excited that I could use this AI as a sidecar developer even an advisory roll. If allowed to have more access ie non demo mode I suspect from this success that the tool could actually write it all.

Blog tags

[openAI](/taxonomy/term/27)

[ChatGPT](/taxonomy/term/23)

[Python](/taxonomy/term/21)

[Jinja](/taxonomy/term/28)

[Flask](/taxonomy/term/29)

[Fabric](/taxonomy/term/30)

[automation](/taxonomy/term/18)

Submitted by auzieman
 on Tue, 02/14/2023 - 08:52

