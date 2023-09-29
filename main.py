from fastapi import FastAPI
import pandas as pd

app = FastAPI()

df_items = pd.read_parquet('src/cleaned/items.parquet')
df_reviews = pd.read_parquet('src/cleaned/reviews.parquet')
df_users = pd.read_parquet('src/cleaned/users.parquet')
df_games = pd.read_parquet('src/cleaned/games.parquet')

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/games")
async def games():
    return {"message": "Games"}

def PlayTimeGenre( genero : str):
    pass
    # Debe devolver `año` con mas horas jugadas para dicho género.
  
# Ejemplo de retorno: {"Año con más horas jugadas para Género X" : 2013}

def UserForGenre( genero : str):
    pass
    # Debe devolver el usuario que acumula más horas jugadas para el género dado y una lista de la acumulación de horas jugadas por año.

# Ejemplo de retorno: {"Usuario con más horas jugadas para Género X" : us213ndjss09sdf,
			    #  "Horas jugadas":[{Año: 2013, Horas: 203}, {Año: 2012, Horas: 100}, {Año: 2011, Horas: 23}]}
       
@app.get("/recommended/{year}")
def UsersRecommend(year):
    """Devuelve el top 3 de juegos MÁS recomendados por usuarios para el año dado. (reviews.recommend = True y comentarios positivos/neutrales)
     Ejemplo de retorno: [{"Puesto 1" : X}, {"Puesto 2" : Y},{"Puesto 3" : Z}]"""
     
    year = int(year)
    top3_ids = df_reviews.loc[(df_reviews['posted'] == year) & (df_reviews['recommend'] == "True") & (df_reviews['sentiment_analysis'] > 0), 'item_id'].value_counts().head(3).to_dict()
    top3 = []
    print(top3_ids)
    for k in top3_ids:
        top3.append(df_games.loc[df_games['id'] == k, 'title'].values.tolist()[0])
    try:    
        return [{"Puesto 1": top3[0]}, {"Puesto 2": top3[1]}, {"Puesto 3": top3[2]}]
    except IndexError:
        return {"Message": "No hay suficientes datos para el año dado"}

@app.get("/notRecommended/{year}")
def UsersRecommend(year):
    """Devuelve el top 3 de juegos MENOS recomendados por usuarios para el año dado. (reviews.recommend = False y comentarios negativos)
     Ejemplo de retorno: [{"Puesto 1" : X}, {"Puesto 2" : Y},{"Puesto 3" : Z}]"""
     
    year = int(year)
    top3_ids = df_reviews.loc[(df_reviews['posted'] == year) & (df_reviews['recommend'] == "False") & (df_reviews['sentiment_analysis'] == 0), 'item_id'].value_counts().head(3).to_dict()
    top3 = []
    print(top3_ids)
    for k in top3_ids:
        top3.append(df_games.loc[df_games['id'] == k, 'title'].values.tolist()[0])
    try:    
        return [{"Puesto 1": top3[0]}, {"Puesto 2": top3[1]}, {"Puesto 3": top3[2]}]
    except IndexError:
        return {"Message": "No hay suficientes datos para el año dado"}


@app.get("/sentiment/{year}")
def sentiment_analysis(year):
    """ Según el año de lanzamiento, se devuelve una lista con la cantidad de registros de reseñas de usuarios que se encuentren categorizados con un análisis de sentimiento. 
    Ejemplo de retorno: {Negative = 182, Neutral = 120, Positive = 278} """
    
    #Generamos una lista de todos los juegos lanzados en el año dado
    year = int(year)
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
    