import React from 'react';
import MapView from './components/Map';

function App() {
  return (
    <div className="relative w-full h-full">
      {/* Main Map */}
      <div className="w-full h-full">
        <MapView />
      </div>
    </div>
  );
}

export default App;
