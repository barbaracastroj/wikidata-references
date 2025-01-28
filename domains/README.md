# Obtención de lista ordenada de dominios más utilizados como referencias en Wikipedia

El objetivo de este script es generar una lista ordenada de los dominios registrados que se utilizan como referencias en Wikipedia. Se debe ordenar desde el dominio más al menos utilizado. Por temas estadísticos, es útil también contar la cantidad de veces que se recurrió a cada dominio.

## Trabajo previo

En su trabajo _External References of English Wikipedia_, Paolo Curotto, extrajo todas las urls utilizadas como referencias externas en los artículos de Wikipedia y guardó los resultados en documentos específicos. Dichos documentos pueden obtenerse desde https://zenodo.org/records/4001139, ahí mismo se entrega la descripción de cada documento y algunas estadísticas importantes. El que nos conviene para nuestro propósito es el archivo **ref-wiki-en_urls.txt.gz** que contiene enlaces directos únicos a referencias externas utilizados en Wikipedia, un link por cada fila, ordenados alfabéticamente.

Este es un extracto del archivo:

```
http://about.usc.edu/facts/
http://about.usc.edu/facts//
http://about.usc.edu/faculty/named-chairs-and-professorships/
http://aboutworldlanguages.com/sindhi
http://abscbnpr.com/national-tv-ratings-august-26-29-2016/
http://abscbnpr.com/national-tv-ratings-august-30-2016-tuesday/
http://abstracts.aetransport.org/paper/index/id/21/confid/1
http://abyss.uoregon.edu/~js/ast121/lectures/lec21.html
http://abyss.uoregon.edu/~js/images/pluto_orient.jpg
```

## Obtención de dominios más utilizados como referencias

Dado que cada fila es una url de una página de un sitio web (dominio registrado), es necesario hacer un trabajo extra para, primero obtener el dominio principal registrado de cada url, y luego contar sus repeticiones.

Para el extracto anterior el primer procesamiento entregaría el siguiente resultado:

```
usc.edu
usc.edu
usc.edu
aboutworldlanguages.com
abscbnpr.com
abscbnpr.com
aetransport.org
uoregon.edu
uoregon.edu
```

Lo siguiente entonces es contar cada dominio principal registrado, lo que entrega:

```
usc.edu: 3
aboutworldlanguages.com: 1
abscbnpr.com: 2
aetransport.org: 1
uoregon.edu: 2
```

Finalmente, ordenar desde el más utilizado al menos utilizado:

```
usc.edu: 3
abscbnpr.com: 2
uoregon.edu: 2
aboutworldlanguages.com: 1
aetransport.org: 1
```

Para realizar lo anterior, en el script se recurre a la librería `tldextract` que extrae el subdominio, el dominio registrado y el TLD (Top-Level Domain) de una url.

## Ejecución script

Para generar la lista ordenada de dominios principales registrados es necesario correr el comando:

`python top_domain_sorter.py`

Esto generará como output dos archivos: `top_domains.json` y `top_domains.txt`. El primero es un archivo con los dominios ordenados y la cantidad de veces que se utilizó cada uno. El segundo, es un archivo de texto plano, también con los dominios ordenados, donde cada fila contiene únicamente el dominio principal registrado, esto es por simplicidad para el siguiente procesamiento de estos datos.
