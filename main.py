from fastapi import FastAPI

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


def PlayTimeGenre( genero : str):
    pass
    # Debe devolver `año` con mas horas jugadas para dicho género.
  
# Ejemplo de retorno: {"Año con más horas jugadas para Género X" : 2013}

def UserForGenre( genero : str):
    pass
    # Debe devolver el usuario que acumula más horas jugadas para el género dado y una lista de la acumulación de horas jugadas por año.

# Ejemplo de retorno: {"Usuario con más horas jugadas para Género X" : us213ndjss09sdf,
			    #  "Horas jugadas":[{Año: 2013, Horas: 203}, {Año: 2012, Horas: 100}, {Año: 2011, Horas: 23}]}

def UsersRecommend( año : int):
    pass
#    Devuelve el top 3 de juegos MÁS recomendados por usuarios para el año dado. (reviews.recommend = True y comentarios positivos/neutrales)
  
# Ejemplo de retorno: [{"Puesto 1" : X}, {"Puesto 2" : Y},{"Puesto 3" : Z}]

def UsersNotRecommend( año : int):
    pass
#    Devuelve el top 3 de juegos MENOS recomendados por usuarios para el año dado. (reviews.recommend = False y comentarios negativos)
  
# Ejemplo de retorno: [{"Puesto 1" : X}, {"Puesto 2" : Y},{"Puesto 3" : Z}]

def sentiment_analysis( año : int):
    pass
    # Según el año de lanzamiento, se devuelve una lista con la cantidad de registros de reseñas de usuarios que se encuentren categorizados con un análisis de sentimiento. 

# Ejemplo de retorno: {Negative = 182, Neutral = 120, Positive = 278}