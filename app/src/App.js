import React, { useState, useEffect, useRef } from 'react';
import StatementQuerier from './components/statement-querier/StatementQuerier';
import Loader from './components/loader/Loader';
import { get_wikidata_item } from './queries/get_wikidata_item';

import './App.css';

const App = () => {
    const [itemQInput, setItemQInput]   = useState('');
    const [itemQ, setItemQ]             = useState('');
    const [data, setData]               = useState('');
    const [currentItem, setcurrentItem] = useState({ name: '', description: '', also_known_as: [] });
    const [loading, setLoading]         = useState(false);
    const inputRef = useRef(null);

    useEffect(() => { inputRef.current.focus() }, []);

    async function queryFlask() {
        setLoading(true);
        setcurrentItem({ name: '', description: '' });
        setItemQ(itemQInput);
        const [name, description, also_known_as, statements, status] = await get_wikidata_item(itemQInput);
        setData(statements);
        setcurrentItem({ name, description, also_known_as });
        setLoading(false);
        if (!status) {
            console.log('Error trying to fetch wikidata item statements');
        }
    }

    const noItemFound = (<div className="no-item-found">No item found</div>);

    return (
        <div className="App">
            <div className="App-header"><p>Wikidata refs</p></div>
            <div className="main-content">
                <div className="title">Search Wikidata item by Identifier</div>
                <div className="search-box">
                    <div className="Q">Q</div>
                    <input value={itemQInput} ref={inputRef} onChange={(e) => setItemQInput(e.target.value.replace(/\D/, ''))} className="q-input" />
                </div>
                <button onClick={queryFlask} className="search-statements-button">Search</button>


                {
                    (loading === true ? <Loader /> :
                        (data === '' ? '' :
                            data.length === 0 ? noItemFound :

                                <>
                                    <div className="info-box">
                                        <div className="item-text-box">
                                            <div className="item-text"> Item name: </div>
                                            <div className="item-text-cap">{currentItem.name}</div>
                                        </div>
                                        <div className="item-text-box">
                                            <div className="item-text"> Description: </div>
                                            <div className="item-text-cap">{currentItem.description}</div>
                                        </div>
                                    </div>

                                    <div className="table-wrapper">
                                        <div className="statements-title">Statements</div>

                                        {
                                            data.map((entry, index) => {
                                                const property        = entry['property']['value'];
                                                const property_number = entry.propNumber.value;
                                                const value           = entry.property_value_Label.value;
                                                const value_url       = entry.property_value_;
                                                const references      = entry.references.value;

                                                return <StatementQuerier
                                                    key={index}
                                                    Q={itemQ}
                                                    name={currentItem.name}
                                                    also_known_as={currentItem.also_known_as}
                                                    property={property}
                                                    property_number={property_number}
                                                    value={value}
                                                    value_url={value_url}
                                                    references={references}
                                                />
                                            })
                                        }
                                    </div>
                                </>
                        )
                    )
                }
            </div>
        </div>
    );
}

export default App;
