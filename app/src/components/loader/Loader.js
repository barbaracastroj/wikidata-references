import React from 'react'
import './Loader.css'

const Loader = () => {

    return (
        <div className="spinner">
            <div className="spinner-container">
                <div className="spinner-layer">
                    <div className="circle-clipper left">
                        <div className="circle">
                        </div>
                    </div>
                    <div className="circle-clipper right">
                        <div className="circle">
                        </div>
                    </div>
                </div>
            </div>
        </div>
    )

}

export default Loader