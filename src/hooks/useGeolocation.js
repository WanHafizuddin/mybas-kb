import { useEffect, useState } from 'react';

// Tracks the browser's own GPS position continuously (not the buses' GPS --
// this is "where am I", for the "locate me" button and the you-are-here dot).
export function useGeolocation() {
    const [position, setPosition] = useState(null);
    const [error, setError] = useState(null);

    useEffect(() => {
        if (!('geolocation' in navigator)) {
            setError('Geolocation is not supported by this browser');
            return;
        }

        const watchId = navigator.geolocation.watchPosition(
            (pos) => {
                setPosition({
                    lat: pos.coords.latitude,
                    lon: pos.coords.longitude,
                    accuracy: pos.coords.accuracy,
                });
                setError(null);
            },
            (err) => setError(err.message),
            { enableHighAccuracy: true, maximumAge: 10000, timeout: 15000 }
        );

        return () => navigator.geolocation.clearWatch(watchId);
    }, []);

    return { position, error };
}
