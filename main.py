from fastapi import FastAPI
from fastapi.responses import HTMLResponse, FileResponse
import pandas as pd

app = FastAPI()

print("Cargando datos...")
try:
    df_items = pd.read_parquet('./src/cleaned/items.parquet', engine='fastparquet')
    df_reviews = pd.read_parquet('./src/cleaned/reviews.parquet' , engine='fastparquet')
    df_users = pd.read_parquet('./src/cleaned/users.parquet' , engine='fastparquet')
    df_games = pd.read_parquet('./src/cleaned/games.parquet' , engine='fastparquet')
    df_genres = pd.read_parquet('./src/cleaned/genres.parquet' , engine='fastparquet')
    df_user_distance = pd.read_parquet('./src/cleaned/user_distance_matrix.parquet' , engine='fastparquet')
except Exception as error:
    print("Error al cargar los datos: ", error)
print("Datos cargados.")



@app.get('/favicon.png', include_in_schema=False)
async def favicon():
    return FileResponse('src/favicon.png')

@app.get("/", response_class=HTMLResponse)
async def root():
    return """
    <html>
        <head>
            <title>Machine Learning Ops</title>
            <link rel="preconnect" href="https://fonts.googleapis.com">
            <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
            <link href="https://fonts.googleapis.com/css2?family=Sen:wght@400;700&display=swap" rel="stylesheet">
            <link id="favicon" rel="icon" type="image/x-icon" href="favicon.png">
        </head>
        <body style="background-color: #fafafa; display: flex; justify-content: center; align-items: center;">
            <h1 style="font-family: 'Sen', sans-serif;">
            Bienvenido al trabajo Machine Learning Ops! La información sobre los endpoints de la API se encuentra en 
            <a href="https://github.com/oOOoutsiderOOo/MLOps_Henry">GitHub</a>
            </h1>
        </body>
    </html>
    """
    
    # return {"message": "Bienvenido al trabajo Machine Learning Ops. Información sobre los endpoints de la API se encuentra en https://github.com/oOOoutsiderOOo/MLOps_Henry"}


@app.get("/playTimeGenre/{genre}")
def PlayTimeGenre( genre : str):
    """
    Debe devolver `año` con mas horas jugadas para dicho género.
  
    Ejemplo de retorno: {"Año con más horas jugadas para Género X" : 2013}
    """
        #Chequeamos que el género exista
    if genre not in df_genres['url_genre'].values.tolist():
        return {"Message": "El género ingresado no existe, la lista de géneros es: " + str(df_genres['url_genre'].values.tolist())}
    
    #Recuperamos el nombre formateado del género
    genre = df_genres.loc[df_genres['url_genre'] == genre, 'genre'].values.tolist()[0]
    
    #Generamos una lista de todos los juegos del género
    game_id_list = df_games.loc[df_games['genres'].str.contains(genre), 'id'].values.tolist()
    
    #Filtramos el dataframe de items para que solo contenga los juegos del género y combinamos con el dataframe de juegos para obtener el año de lanzamiento
    df_items_of_genre = df_items.loc[df_items['item_id'].isin(game_id_list)]
    df_items_of_genre = df_items_of_genre.merge(df_games[['release_date', 'id']], left_on='item_id', right_on='id').drop(columns=['id', 'item_name', 'user_id', 'item_id'])
    
    #Agrupamos por año y sumamos las horas jugadas
    df_items_of_genre = df_items_of_genre.groupby('release_date').sum().reset_index()
    
    #Seleccionamos el año con más horas jugadas
    year = df_items_of_genre.sort_values(by=['playtime_forever'], ascending=False).head(1)['release_date'].values.tolist()[0]
    
    return {"Año con más horas jugadas para género " + genre : year}

@app.get("/userForGenre/{genre}")
def UserForGenre(genre):
    """
    Debe devolver el usuario que acumula más horas jugadas para el género dado y una lista de la acumulación de horas jugadas por año.

    Ejemplo de retorno: {"Usuario con más horas jugadas para Género X" : us213ndjss09sdf, 
    "Horas jugadas":[{Año: 2013, Horas: 203}, {Año: 2012, Horas: 100}, {Año: 2011, Horas: 23}]}
    """
    
    #Chequeamos que el género exista
    if genre not in df_genres['url_genre'].values.tolist():
        return {"Message": "El género no existe, la lista de géneros es: " + str(df_genres['url_genre'].values.tolist())}
    
    #Recuperamos el nombre formateado del género
    genre = df_genres.loc[df_genres['url_genre'] == genre, 'genre'].values.tolist()[0]
    
    #Generamos una lista de todos los juegos del género
    game_id_list = df_games.loc[df_games['genres'].str.contains(genre), 'id'].values.tolist()
    
    #Filtramos el dataframe de items para que solo contenga los juegos del género y agrupamos por usuario
    df_items_of_genre = df_items.loc[df_items['item_id'].isin(game_id_list)]
    df_items_of_genre = df_items_of_genre.drop(columns=['item_id', 'item_name'])
    df_items_of_genre = df_items_of_genre.groupby('user_id').sum().reset_index()

    #Seleccionamos el usuario con más horas jugadas
    player = df_items_of_genre.sort_values(by=['playtime_forever'], ascending=False).head(1)['user_id'].values.tolist()[0]
    
    #Creamos una tabla con los juegos , el año de lanzamiento y las horas jugadas
    df_games_of_user = df_items.loc[(df_items['user_id'] == player) & (df_items['item_id'].isin(game_id_list))]
    df_games_of_user = df_games_of_user.merge(df_games[['release_date', 'id']], left_on='item_id', right_on='id').drop(columns=['id', 'item_name', 'user_id', 'item_id'])
    
    #Agrupamos por año y sumamos las horas jugadas
    df_games_of_user = df_games_of_user.groupby('release_date').sum().reset_index()
    
    #Ceamos una lista de diccionarios con el año y las horas jugadas
    years_playtime = []
    for i, j in df_games_of_user.iterrows():
        year = j['release_date']
        playtime = j['playtime_forever']
        years_playtime.append({"Año": year, "Horas": playtime})
        
    #Devolvemos el usuario con más horas jugadas y la lista de años y horas jugadas      
    return ({"Usuario con más horas jugadas para el género " + genre : player}, {"Horas jugadas": eval(str(years_playtime))})
       
       
@app.get("/recommended/{year}")
def UsersRecommend(year):
    """
    Devuelve el top 3 de juegos MÁS recomendados por usuarios para el año dado. (reviews.recommend = True y comentarios positivos/neutrales).
    
    Ejemplo de retorno: [{"Puesto 1" : X}, {"Puesto 2" : Y},{"Puesto 3" : Z}]
    """
     
    #Generamos una lista de los primeros ids de juegos más recomendados para el año dado
    try:
        year = int(year)
    except:
        return {"Message": "El año ingresado no es válido"}
    
    top3_ids = df_reviews.loc[(df_reviews['posted'] == year) & (df_reviews['recommend'] == "True") & (df_reviews['sentiment_analysis'] > 0), 'item_id'].value_counts().head(3).to_dict()
    
    #Buscamos los nombres de los juegos en la lista de ids
    top3 = []
    for k in top3_ids:
        top3.append(df_games.loc[df_games['id'] == k, 'title'].values.tolist()[0])
    
    #Devolvemos una lista con los nombres de los juegos
    try:    
        return [{"Puesto 1": top3[0]}, {"Puesto 2": top3[1]}, {"Puesto 3": top3[2]}]
    except IndexError:
        return {"Message": "No hay suficientes datos para el año dado"}
    

@app.get("/notRecommended/{year}")
def UsersRecommend(year):
    """
    Devuelve el top 3 de juegos MENOS recomendados por usuarios para el año dado. (reviews.recommend = False y comentarios negativos).
    
    Ejemplo de retorno: [{"Puesto 1" : X}, {"Puesto 2" : Y},{"Puesto 3" : Z}]
    """
     
    #Generamos una lista de los primeros ids de juegos menos recomendados para el año dado
    try:
        year = int(year)
    except:
        return {"Message": "El año ingresado no es válido"}
    top3_ids = df_reviews.loc[(df_reviews['posted'] == year) & (df_reviews['recommend'] == "False") & (df_reviews['sentiment_analysis'] == 0), 'item_id'].value_counts().head(3).to_dict()
    
    #Buscamos los nombres de los juegos en la lista de ids
    top3 = []
    for k in top3_ids:
        top3.append(df_games.loc[df_games['id'] == k, 'title'].values.tolist()[0])

    #Devolvemos una lista con los nombres de los juegos
    try:    
        return [{"Puesto 1": top3[0]}, {"Puesto 2": top3[1]}, {"Puesto 3": top3[2]}]
    except IndexError:
        return {"Message": "No hay suficientes datos para el año dado"}


@app.get("/sentiment/{year}")
def sentiment_analysis(year):
    """ 
    Según el año de lanzamiento, se devuelve una lista con la cantidad de registros de reseñas de usuarios que se encuentren categorizados con un análisis de sentimiento. 
    
    Ejemplo de retorno: {Negative = 182, Neutral = 120, Positive = 278} 
    """
    
    #Generamos una lista de todos los juegos lanzados en el año dado
    try:
        year = int(year)
    except:
        return {"Message": "El año ingresado no es válido"}
    game_id_list = df_games[df_games['release_date'] == year]['id'].tolist()

    #Agregamos a una lista todos los análisis de sentimiento de cada juego
    sentiment_list = []
    for game_id in game_id_list:
        sentiment_list += (df_reviews.loc[df_reviews['item_id'] == game_id, 'sentiment_analysis'].values.tolist())
    
    #Calculamos las sumas de cada categoría de análisis de sentimiento
    negative = sentiment_list.count(0)
    neutral = sentiment_list.count(1)
    positive = sentiment_list.count(2)
    
    #Devolvemos un diccionario con las sumas
    return {"Negative" : negative, "Neutral" : neutral, "Positive" : positive}

@app.get("/recommendByUser/{user}")
def getRecommendations(user : str):
    """
    Devuelve el top 5 de juegos recomendados para el usuario dado.
    """
    
    #Creamos una lista con los usuarios ordenados por similitud con el usuario dado. A la vez
    #devolvemos un error si el usuario no existe
    try:
        user_list = df_user_distance[user].sort_values(ascending=True).head(100).index.tolist()
        user_list.remove(user)
    except:
        return {"Message": "El usuario ingresado no existe, la lista de usuarios es: " + str(df_users['user_id'].values.tolist())}
    
    #Creamos un dataframe con los aproximadamente 100 juegos jugados por los usuarios más similares
    games_list = set()
    for user in user_list:
        if len(games_list) > 100:
            break
        games_list.update(df_items.loc[df_items['user_id'] == user, 'item_id'].values.tolist())
    
    df_top_games = df_games.loc[df_games['id'].isin(games_list)]
    
    #Ordenamos por metascore y devolvemos el top 5   
    top_games = df_top_games.sort_values(by=['metascore'], ascending=False).head(5)['title'].values.tolist()
    
    return {"Los 5 juegos recomendados para este usuario son:" :top_games}

@app.get("/worstDevByYear/{year}")
def UsersWorstDeveloper(year): 
    """Devuelve el top 3 de desarrolladoras con juegos MENOS recomendados por usuarios para el año dado. (reviews.recommend = False y comentarios negativos)
    
    Ejemplo de retorno: [{"Puesto 1" : X}, {"Puesto 2" : Y},{"Puesto 3" : Z}]
    """
    
    #Generamos una lista de los primeros ids de juegos más recomendados para el año dado
    try:
        year = int(year)
    except:
        return {"Message": "El año ingresado no es válido"}
    
    #Agregamos la columna de desarrolladora al dataframe de reseñas, luego creamos columnas para cada categoría de análisis de sentimiento y filtramos por año
    df_reviews_with_dev = df_reviews.merge(df_games[['developer', 'id']], left_on='item_id', right_on='id').drop(columns=['id'])
    df_reviews_with_dev = pd.get_dummies(df_reviews_with_dev, columns=['recommend', 'sentiment_analysis'])
    df_reviews_with_dev = df_reviews_with_dev.loc[df_reviews_with_dev['posted'] == year]
    
    
    df_devs_reviews = df_reviews_with_dev.groupby('developer').sum().reset_index().sort_values(by=['recommend_False', 'sentiment_analysis_0'], ascending=False).head(3)
    devs = df_devs_reviews['developer'].values.tolist()
    
    try:    
        return [{"Puesto 1": devs[0]}, {"Puesto 2": devs[1]}, {"Puesto 3": devs[2]}]
    except IndexError:
        return {"Message": "No hay suficientes datos para el año dado"}
    

@app.get("/devSentiment/{dev}")
def sentiment_analysis( dev : str ):
    """
    Según la empresa desarrolladora, se devuelve un diccionario con el nombre de la desarrolladora como llave y una lista con la cantidad total de registros 
    de reseñas de usuarios que se encuentren categorizados con un análisis de sentimiento como valor.
    
    Ejemplo de retorno: {'Valve' : [Negative = 182, Neutral = 120, Positive = 278]}
    """
    
    #Agregamos la columna de desarrolladora al dataframe de reseñas, luego creamos columnas para cada categoría de análisis de sentimiento y agrupamos por desarrolladora
    df_reviews_with_dev = df_reviews.merge(df_games[['developer', 'id']], left_on='item_id', right_on='id').drop(columns=['id'])
    df_reviews_with_dev = pd.get_dummies(df_reviews_with_dev, columns=['sentiment_analysis'])
    df_devs_reviews = df_reviews_with_dev.groupby('developer').sum().reset_index()
    df_devs_reviews['dev_lower'] = df_devs_reviews['developer'].str.lower()
    
    if dev.lower() not in df_devs_reviews['dev_lower'].values.tolist():
        return {"Message": "El nombre ingresado no existe, la lista de desarrolladoras es: " + str(df_devs_reviews['developer'].values.tolist())}
    
    negative = df_devs_reviews.loc[df_devs_reviews['dev_lower'] == dev.lower(), 'sentiment_analysis_0'].values.tolist()[0]
    neutral = df_devs_reviews.loc[df_devs_reviews['dev_lower'] == dev.lower(), 'sentiment_analysis_1'].values.tolist()[0]
    positive = df_devs_reviews.loc[df_devs_reviews['dev_lower'] == dev.lower(), 'sentiment_analysis_2'].values.tolist()[0]
    
    return {dev : {'Negative': negative, 'Neutral': neutral, 'Positive': positive}}
    