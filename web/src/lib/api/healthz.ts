import { API_BASE } from './utils';

export type StatusResponse = {
	status: string;
};

export async function getHealthz() {
	const res = await fetch(`${API_BASE}/api/v1/healthz`);
	const data = await res.json();
	return data as StatusResponse;
}
