# Machine Learning Ops - Henry


## Descripción

El presente es es un proyecto para el bootcamp de Data Science de Henry. En éste se exponen una serie de endpoints que permiten acceder a información sobre usuarios y juegos de la plataforma Steam. Para lograrlo se ha hecho un proceso de ETL sobre los datos de la plataforma, se ha entrenado un modelo de Machine Learning y se ha desplegado una API con FastAPI.

***

### Deployment

El proyecto se encuentra desplegado en Railway, en el siguiente link:
https://mlopshenry-production.up.railway.app/

***

### Endpoints

Se puede acceder a los siguientes endpoints reemplazando el final de la URL por el dato que se desee consultar.

&nbsp;  
https://mlopshenry-production.up.railway.app/playTimeGenre/`género`

Devuelve el año con más horas jugadas para el género ingresado.
 
&nbsp;  
https://mlopshenry-production.up.railway.app/userForGenre/`género`

Devuelve el usuario con más horas jugadas para el género ingresado, y un desglose de las horas jugadas por cada año
 
&nbsp;  
https://mlopshenry-production.up.railway.app/recommended/`año`

Devuelve los juegos más recomendados para el año ingresado. Estos se calcularon realizando un análisis de sentimiento sobre las reviews de los usuarios.
 
&nbsp;  
https://mlopshenry-production.up.railway.app/notRecommended/`año`

Devuelve los juegos menos recomendados para el año ingresado. Se calcularon de la misma manera que los más recomendados.
 
&nbsp;  
https://mlopshenry-production.up.railway.app/sentiment/`año`

Devuelve un desglose de los resultados del análisis de sentimiento para el año ingresado.

&nbsp;  
https://mlopshenry-production.up.railway.app/recommendByUser/`usuario`

Devuelve los juegos más recomendados para el usuario ingresado. Para ello primero se calculó una matriz de distancia entre usuarios a partir de las reviews realizadas por los mismos. Luego se calculó la distancia entre el usuario ingresado y el resto de los usuarios, y se seleccionaron los 100 primeros juegos de los usuarios mas cercanos. Finalmente se ordenaron por metascore y se devuelven los 5 primeros.
Es importante aclarar que sólo se consideraron los usuarios que hayan escrito al menos una review.
