import axios from "axios";

const ES_URL = "https://localhost:9200/wikidata-references/_search";
const ES_AUTH = {
  username: "elastic",
  password: "elastic-password",
};

const axiosConfig = {
  method: "post",
  url: ES_URL,
  auth: ES_AUTH,
  headers: { "Content-Type": "application/json" },
  ...(typeof window === "undefined"
    ? {
        httpsAgent: new (await import("https")).Agent({
          rejectUnauthorized: false,
        }),
      }
    : {}), // Solo usa httpsAgent en Node.js
};

export async function get_solr_docs(
  name,
  property,
  value,
  Q,
  limitSearch,
  altLabelsData
) {
  let docs = [];
  let status = false;

  const { altLabels, item_altLabels, prop_altLabels, val_altLabels } =
    altLabelsData;

  let alt_labels_string = "";
  if (altLabels) {
    alt_labels_string += appendLabels(item_altLabels);
    alt_labels_string += appendLabels(prop_altLabels);
    alt_labels_string += appendLabels(val_altLabels);
  }

  const limit_query = limitSearch ? { match: { Q: Q } } : null;

  let data_to_query = `${name} ${property} ${alt_labels_string} ${value}`;
  data_to_query = data_to_query.replace(/:/g, "");

  const query = {
    size: 3,
    query: {
      bool: {
        must: [{ match: { content: data_to_query } }],
        ...(limit_query ? { filter: [limit_query] } : {}),
      },
    },
    _source: ["warc-target-uri", "content"],
    highlight: {
      fields: {
        content: {},
      },
    },
  };

  try {
    const es_res = await axios({ ...axiosConfig, data: JSON.stringify(query) });

    console.log(es_res);

    for (let hit of es_res.data.hits.hits) {
      docs.push({
        url: hit._source["warc-target-uri"],
        highlight: hit.highlight?.content?.[0] || "",
      });
    }
    status = true;
  } catch (error) {
    console.error("Error en la consulta a Elasticsearch:", error);
    status = false;
  }

  return [docs, status];
}

function appendLabels(labels_arr) {
  return labels_arr.reduce((acc, label) => acc + label + " ", "");
}
