import axios from 'axios';


const WIKIDATA_URL = 'https://query.wikidata.org/sparql';


export async function get_similar_terms(property_number, type, value_url) {
    
    const value_query = createValueSubQuery(type, value_url);
    const query = createQuery(property_number, value_query);
    
    let prop_alt_labels = [];
    let value_alt_labels = [];
    let status = false;

    try {
        const wikidata_res = await axios({
            method: 'get',
            url: WIKIDATA_URL,
            headers: { 'Accept': 'application/sparql-results+json' },
            params: { 'format': 'json', 'query': query },
        });

        const response_data = createResponseData(wikidata_res);
        prop_alt_labels = response_data.prop_alt_labels;
        value_alt_labels = response_data.value_alt_labels;
        status = true;
    }
    catch (error) { status = false }

    return [prop_alt_labels, value_alt_labels, status];
}

function createValueSubQuery(type, url) {
    if (type !== 'uri') {
        return '';
    }
    const value_identifier = url.slice(32);
    return `UNION {
        BIND("value"@en AS ?item ) .
        OPTIONAL { 
            wd:Q${value_identifier} skos:altLabel ?also_known_as .
            FILTER (LANG(?also_known_as) = "en") 
        }  
    }`
}

function createQuery(property_number, value_sub_query) {
    return `SELECT ?item ?also_known_as {
        {
            BIND("property"@en AS ?item ) .
            OPTIONAL { 
                wd:P${property_number} skos:altLabel ?also_known_as .
                FILTER (LANG(?also_known_as) = "en") 
            }  
        }
        ${value_sub_query} 
    }`
}

function createResponseData(wikidata_data) {
    const response_data = {
        prop_alt_labels: [],
        value_alt_labels: [],
    }

    for (let row of wikidata_data['data']['results']['bindings']) {
        try {
            const item = row['item']['value'];
            const label = row['also_known_as']['value'];

            if (label === undefined) { continue }
            if (item === 'property') { response_data['prop_alt_labels'].push(label) }
            if (item === 'value') { response_data['value_alt_labels'].push(label) }
            
        } catch (err) {
            continue;
        }
    }

    return response_data;
}