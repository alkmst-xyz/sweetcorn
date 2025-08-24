import { getHealthz } from '$lib/api';
import type { LayoutLoad } from './$types';

export const load: LayoutLoad = async ({ fetch }) => {
	return {
		healthz: await getHealthz(fetch)
	};
};
