import React from 'react'
import './Loader.css'

const LoaderSmall = () => {

    return (
        <div className="spinner">
            <div className="spinner-container-small">
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

export default LoaderSmall