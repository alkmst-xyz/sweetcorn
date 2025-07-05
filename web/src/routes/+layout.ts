import type { StatusResponse } from '$lib/api';
import type { LayoutLoad } from './$types';

// SPA mode
export const ssr = false;

export const load: LayoutLoad = async ({ fetch }) => {
	const res = await fetch('http://127.0.0.1:4318/');
	const data = await res.json();
	return data as StatusResponse;
};
