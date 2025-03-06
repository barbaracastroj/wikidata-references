import axios from 'axios';


export async function get_wikidata_item(item_number) {
    let name = '';
    let description = '';
    let also_known_as = [];
    let statements = [];
    let status = false ;
    try {
        const res = await axios({
            method: 'get',
            url: '/wikidata',
            params: { item: item_number },
        });
        name          = res.data.name;
        description   = res.data.description;
        also_known_as = res.data.item_labels;
        statements    = res.data.statements;
        status = true;

    } catch (error) { status = false }

    return [name, description, also_known_as, statements, status];
}


export async function get_solr_docs(name, property, value, Q, limitSearch, altLabelsData) {
    let docs = [];
    let status = false;
    try {
        const res = await axios({
            method: 'post',
            url: '/solr/',
            data: {
                name: name,
                property: property,
                value: value,
                Q: Q,
                limitToItemUrls: limitSearch,
                altLabelsData: altLabelsData,
            },
            headers: { 'Content-Type': 'application/json' },
        });
        docs = res.data.docs
        status = true
    } catch (error) { status = false }

    return [docs, status];
}


export async function get_similar_terms(property_number, type, value_url) {
    let prop_alt_labels = [];
    let value_alt_labels = [];
    let status = false;
    try {
        const res = await axios({
            method: 'post',
            url: '/wikidata/search_similar_terms',
            data: {
                prop_number: property_number,
                val_type: type,
                val_url: value_url,
            },
            headers: { 'Content-Type': 'application/json' },
        });
        status = true;
        prop_alt_labels = res.data.prop_alt_labels;
        value_alt_labels = res.data.value_alt_labels;
    } catch (error) { status = false }

    return [prop_alt_labels, value_alt_labels, status];
}
