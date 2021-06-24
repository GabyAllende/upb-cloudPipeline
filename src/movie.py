
import json
import boto3
import os
from boto3.dynamodb.conditions import Key, Attr


movies_table = os.environ['MOVIES_TABLE']

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(movies_table)

def getMovie(event, context):
    print(json.dumps({"running": True}))
    print(json.dumps(event))
    
    path = event["path"] # "/user/123"
    array_path = path.split("/") # ["", "user", "123"]
    movie_id = array_path[-1]
    print("temp")
    response = table.get_item(
        Key={
            'pk': movie_id,
            'sk': 'mov_info'
        }
    )
    item = response['Item']
    print(item)
    return {
        'statusCode': 200,
        'body': json.dumps(item)
    }
    
def putMovie(event, context):
    print(json.dumps({"running": True}))
    print(json.dumps(event))
    
    path = event["path"] # "/user/123"
    array_path = path.split("/") # ["", "user", "123"]
    movie_id = array_path[-1]
    
    body = event["body"] #"{\n\t\"name\": \"Jack\",\n\t\"last_name\": \"Click\",\n\t\"age\": 21\n}"
    body_object = json.loads(body)
    
    print(body_object['main_actors'])
    print(body_object['title'])
    print(body_object['year'])
    
    res = table.put_item(
        Item={
            'pk': movie_id,
            'sk': 'mov_info',
            'main_actors': body_object['main_actors'],
            'title': body_object['title'],
            'year': body_object['year']
        })
     
    return {
        'statusCode': 200,
        'body': json.dumps("Agregado correctamente")
    }

def roomsPerDay(event, context):
    print(json.dumps({"running": True}))
    print(json.dumps(event))
    
    path = event["path"] # "/user/123"
    array_path = path.split("/") # ["", "user", "123"]
    user_id = array_path[-1]
    
    # response = table.get_item(
    #     Key={
    #         'pk': user_id,
    #         'sk': 'age'
    #     }
    # )
    # item = response['Item']
    # print(item)
    return {
        'statusCode': 200,
        'body': json.dumps("success")
    }
    
def getAvailableRooms(event, context):
    print(json.dumps({"running": True}))
    print(json.dumps(event))
    
    path = event["path"] # "/user/123"
    array_path = path.split("/") # ["", "user", "123"]
    movie_id = array_path[-1]
    print("Llega hasta aca")
    
    table2 = table.scan(FilterExpression=Attr('pk').eq(movie_id) & Attr('sk').begins_with("room_"))
    data = table2['Items']
    print("data")
    # response = table.get_item(
    #     Key={
    #         'pk': user_id,
    #         'sk': 'age'
    #     }
    # )
    # item = response['Item']
    # print(item)
    return {
        'statusCode': 200,
        'body': json.dumps(data)
    }
    
def getAudience(event, context):
    print(json.dumps({"running": True}))
    print(json.dumps(event))
    
    path = event["path"] # "/user/123"
    array_path = path.split("/") # ["", "user", "123"]/ "/rooms/{room_id}/movies/{movie_id}"
    movie_id = array_path[-1]
    room_id = array_path[-3]
    print("Llega hasta aca")
    
    q1 = table.scan(FilterExpression=Attr('pk').eq(movie_id) & Attr('sk').eq(room_id))
    q2 = table.scan(FilterExpression=Attr('pk').eq(room_id) & Attr('sk').begins_with("person_"))
    data1 = q1['Items']
    data2 = q2['Items']
    
    temp = []
    
    for x in data1:
        for y in data2:
            if x['schedule'] == y['schedule']:
                temp.append(y)
    
    
    return {
        'statusCode': 200,
        'body': json.dumps(temp)
    }
    
    
def getAvailableSeats3D(event, context):
    print(json.dumps({"running": True}))
    print(json.dumps(event))
    
    path = event["path"] # "/user/123"
    array_path = path.split("/") # ["", "user", "123"]
    room_id = array_path[-1]
    
    
    q1 = table.scan(FilterExpression=Attr('pk').eq(room_id) & Attr('sk').eq("room_info"))
    q2 = table.scan(FilterExpression=Attr('pk').eq(room_id) & Attr('sk').begins_with("person_"))
    asientosOcupados = len(q2['Items'])
    capacidadRoom = q1['Items']
    a=""
    print('CapacidadRomm: ',capacidadRoom)
    print('CapacidadRomm[0][capacity]: ',capacidadRoom[0]['capacity'])
    
    
    
    if len(capacidadRoom)>0:
        cap = capacidadRoom[0]['capacity']
        a = "El numero de asientos libres del la sala: "+room_id+" es de "+str(int(cap)-asientosOcupados)
    else:
        a = "No se encontro la informacion para la sala "+room_id
    
    b=""
    if apacidadRoom[0]['3D'] == "yes":
        b =". La sala cuenta con 3D"
    else:
        b =". La sala NO cuenta con 3D"
    
    
    print('CapacidadRomm: ',capacidadRoom)
    
    
    return {
        'statusCode': 200,
        'body': json.dumps(a+b)
    }
    
def getPersonMovies(event, context):
    print(json.dumps({"running": True}))
    print(json.dumps(event))
    
    path = event["path"] # "/user/123"
    array_path = path.split("/") # ["", "user", "123"]/ "/rooms/{room_id}/movies/{movie_id}"
    person_id = array_path[-1]
    
    print("Llega hasta aca")
    
    q1 = table.scan(FilterExpression=Attr('sk').eq(person_id))
    q2 = table.scan(FilterExpression=Attr('pk').begins_with("mov_") & Attr('sk').begins_with("room_"))
    data1 = q1['Items']
    data2 = q2['Items']
    
    temp = []
    
    for x in data1:
        for y in data2:
            if (x['schedule'] == y['schedule']) & (x['pk']==y['sk']):
                q3=table.scan(FilterExpression=Attr('pk').eq(y['pk']) & Attr('sk').eq("mov_info"))
                temp.append(q3['Items'][0])
    
    
    return {
        'statusCode': 200,
        'body': json.dumps(temp)
    }
    
    
def putPeople(event, context):
    print(json.dumps({"running": True}))
    print(json.dumps(event))
    
    path = event["path"] # "/user/123"
    array_path = path.split("/") # ["", "user", "123"]/ "/rooms/{room_id}/movies/{movie_id}"
    room_id = array_path[-1]
    
    body = event["body"] #"{\n\t\"name\": \"Jack\",\n\t\"last_name\": \"Click\",\n\t\"age\": 21\n}"
    body_object = json.loads(body)
    
    people = body_object
    
    print('people: ',people)
    
    q2 = table.scan(FilterExpression=Attr('pk').eq(room_id) & Attr('sk').begins_with("person_"))
    asientosOcupados = len(q2['Items'])
    
    q1 = table.scan(FilterExpression=Attr('pk').eq(room_id) & Attr('sk').eq("room_info"))
    
    
    
    
    print('room: ',q1['Items'])
    print('room2: ',q1['Items'][0]['capacity'])
    
    capacidadRoom = q1['Items'][0]['capacity']
    
    print("Llega hasta despues de los qs")
    
    e=""
    print('int(capacidadRoom)',int(capacidadRoom))
    print('len(people)',len(people))
    print('q1[Items]',q1['Items'])
    print(len(q1['Items']))
    
    if (len(q1['Items'])>0) & ((int(capacidadRoom)-asientosOcupados)>len(people)):
        print("ENTRA AL IF")
        for x in people:
            res = table.put_item(
            Item={
                'pk': x['pk'],
                'sk': x['sk'],
                'schedule': x['schedule'],
                'last_name': x['last_name'],
                'name': x['name']
            })
        e="Personas agregadas correctamente"
    else:
        e="No existen los asientos necesarios para acomodar a todas las personas o no se tienen los datos necesarios"
    
    
    return {
        'statusCode': 200,
        'body': json.dumps(e)
    }
    