import { afterEach, describe, expect, it, vi } from 'vitest';
import { cleanup, render, screen, waitFor } from '@testing-library/react';
import { NextBusPrediction } from './NextBusPrediction';

afterEach(cleanup);

function fakeFetch(body, ok = true) {
    return vi.fn(() => Promise.resolve({ ok, json: () => Promise.resolve(body) }));
}

const base = {
    route_id: '30425', stop_id: '7496',
    // 11:42 UTC = 7:42 pm in Kuala Lumpur
    predicted_arrival: '2026-11-14T11:42:00+00:00',
};

describe('NextBusPrediction', () => {
    it.each([
        ['model', 'predicted'],
        ['live_delay', 'live delay'],
        ['timetable', 'scheduled'],
    ])('shows the predicted local time with the "%s" source label', async (source, label) => {
        const fetchImpl = fakeFetch({ ...base, source });
        render(<NextBusPrediction routeId="30425" stopId="7496" active fetchImpl={fetchImpl} />);
        const el = await screen.findByTestId('next-bus-prediction');
        expect(el.textContent).toMatch(/7:42/);
        expect(el.textContent).toContain(`(${label})`);
        expect(fetchImpl).toHaveBeenCalledWith(expect.stringContaining('/predict-delay?route_id=30425&stop_id=7496'));
    });

    it('does not fetch while its tooltip is closed', () => {
        const fetchImpl = fakeFetch({ ...base, source: 'model' });
        render(<NextBusPrediction routeId="30425" stopId="7496" active={false} fetchImpl={fetchImpl} />);
        expect(fetchImpl).not.toHaveBeenCalled();
        expect(screen.queryByTestId('next-bus-prediction')).toBeNull();
    });

    it('renders nothing when the API fails, so the timetable text stays', async () => {
        const fetchImpl = vi.fn(() => Promise.reject(new Error('offline')));
        render(<NextBusPrediction routeId="30425" stopId="7496" active fetchImpl={fetchImpl} />);
        await waitFor(() => expect(fetchImpl).toHaveBeenCalled());
        expect(screen.queryByTestId('next-bus-prediction')).toBeNull();
    });

    it('renders nothing when there is no prediction (no more trips today)', async () => {
        const fetchImpl = fakeFetch({ ...base, source: 'none', predicted_arrival: null });
        render(<NextBusPrediction routeId="30425" stopId="7496" active fetchImpl={fetchImpl} />);
        await waitFor(() => expect(fetchImpl).toHaveBeenCalled());
        expect(screen.queryByTestId('next-bus-prediction')).toBeNull();
    });

    it('renders nothing on an HTTP error', async () => {
        const fetchImpl = fakeFetch({ detail: 'nope' }, false);
        render(<NextBusPrediction routeId="30425" stopId="7496" active fetchImpl={fetchImpl} />);
        await waitFor(() => expect(fetchImpl).toHaveBeenCalled());
        expect(screen.queryByTestId('next-bus-prediction')).toBeNull();
    });
});
