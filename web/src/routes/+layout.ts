import { API_BASE, type StatusResponse } from '$lib/api';
import type { LayoutLoad } from './$types';

export const load: LayoutLoad = async ({ fetch }) => {
	const res = await fetch(`${API_BASE}/api/v1/healthz`);
	const data = await res.json();
	return data as StatusResponse;
};
