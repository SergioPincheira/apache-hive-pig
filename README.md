# apache-hive-pig

Este proyecto utiliza herramientas del ecosistema Hadoop para procesar y analizar las preferencias musicales de usuarios de la red social de servicios musicales Last.fm. El objetivo es obtener información estadística sobre las canciones y artistas favoritos de los usuarios, y analizar cómo estas preferencias varían según el género y la edad de los usuarios.

## Descripción del Proyecto

Se procesa un conjunto de datos de Last.fm que contiene registros de canciones escuchadas por los usuarios. Se realizan consultas para obtener un ranking de artistas populares y analizar la demografía de los oyentes del artista más popular.

## Datasets
El conjunto de datos incluye dos archivos:

userid-profile.tsv: Información de perfil de 992 usuarios con los siguientes campos:

- userid: Identificador único del usuario.
- gender: Género del usuario (puede estar vacío).
- age: Edad del usuario (puede estar vacío).
- country: País del usuario.
- signup: Fecha de registro del usuario.
  
userid-timestamp-artid-artname-traid-traname.tsv: Registros de canciones escuchadas por los usuarios, con los siguientes campos:

- userid: Identificador del usuario.
- timestamp: Marca temporal de la escucha.
- musicbrainz-artist-id: ID del artista.
- artist-name: Nombre del artista.
- musicbrainz-track-id: ID de la canción.
- track-name: Nombre de la canción.

## Herramientas y Librerías

- Apache Hive: Para consultas SQL en grandes volúmenes de datos.
- Apache Pig: Para procesamiento de datos y consultas avanzadas en el ecosistema Hadoop.
