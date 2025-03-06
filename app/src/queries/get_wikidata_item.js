import axios from 'axios';


const WIKIDATA_URL = 'https://query.wikidata.org/sparql';


export async function get_wikidata_item(item_number) {
    let name = '';
    let description = '';
    let also_known_as = [];
    let statements = [];
    let status = false;

    try {
        const wikidata_res = await axios({
            method: 'get',
            url: WIKIDATA_URL,
            headers: { 'Accept': 'application/sparql-results+json' },
            params: { 'format': 'json', 'query': query(item_number) },
        });

        const parsed_results = parseResults(wikidata_res.data.results.bindings);
        name = parsed_results.name;
        description = parsed_results.description;
        also_known_as = parsed_results.item_labels;
        statements = parsed_results.statements;
        status = true;

    } catch (error) { status = false }

    return [name, description, also_known_as, statements, status];
}

function query(item) {
    return `SELECT ?propNumber ?property ?property_value_Label ?property_value_ (GROUP_CONCAT (DISTINCT ?reference; SEPARATOR=", ") AS ?references) {
        VALUES (?item) {(wd:Q${item})}  {
        BIND("__Name__"@en AS ?property ) .
            ?item rdfs:label ?property_value_ .
            FILTER (LANG(?property_value_) = "en")
        }
  
        UNION {
            BIND("__AlsoKnownAs__"@en AS ?property ) .
            OPTIONAL { ?item skos:altLabel ?property_value_}.
            FILTER (LANG(?property_value_) = "en")
        }
  
        UNION {
            BIND("__Description__"@en AS ?property ) .
            OPTIONAL {?item schema:description ?property_value_}.
            FILTER (LANG(?property_value_) = "en")
        }
  
        UNION {
          ?item ?p ?statement .
          ?statement ?ps ?property_value_ .
          ?property_ rdf:type wikibase:Property .
          ?property_ wikibase:statementProperty ?ps.
          ?property_ rdfs:label ?property .
          FILTER (LANG(?property) = "en")
  
        OPTIONAL {
          ?statement prov:wasDerivedFrom ?ref .
          ?ref pr:P854 ?reference .
        }
  
        BIND(SUBSTR(str(?p), 31, 250) AS ?propNumber)
  
        }
  
        SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
      }
      GROUP BY ?propNumber ?property ?property_value_Label ?property_value_
      ORDER BY xsd:integer(?propNumber) ?property_ ?property_value_`
}

function parseResults(arr_of_results) {
    const final_results = {
        name: '',
        description: '',
        item_labels: [],
        statements: [],
    };

    for (let result of arr_of_results) {

        let value = result['property_value_Label']['value'];

        if (value.startsWith('http://commons.wikimedia.org/')) {
            continue;
        }

        if (isValueADate(value)) {
            const [day, month, year] = getDayMonthYear(value);
            value = day + ' ' + month + ' ' + year;
            result['property_value_Label']['value'] = value;
        }

        if (result.property.value === '__Name__') {
            final_results.name = value;
            continue;
        }

        if (result.property.value === '__Description__') {
            final_results.description = value;
            continue;
        }

        if (result.property.value === '__AlsoKnownAs__') {
            final_results.item_labels.push(value);
            continue;
        }

        final_results.statements.push(result);

    }
    return final_results;
}

function isValueADate(val) {
    return val.charAt(4) === '-' && val.charAt(7) === '-' && val.charAt(10) === 'T' && val.charAt(13) === ':' && val.charAt(16) === ':' && val.charAt(19) === 'Z';
}

function getDayMonthYear(date) {
    let day = date.substring(8, 10);
    let month = date.substring(5, 7);
    let year = date.substring(0, 4);

    if (day[0] === '0') {
        day = day.substring(1, 2);
    }
    const months = {
        '01': 'January',
        '02': 'February',
        '03': 'March',
        '04': 'April',
        '05': 'May',
        '06': 'June',
        '07': 'July',
        '08': 'August',
        '09': 'September',
        '10': 'October',
        '11': 'November',
        '12': 'December',
    }

    if (months.hasOwnProperty(month)) {
        month = months[month];
    }

    return [day, month, year];
}