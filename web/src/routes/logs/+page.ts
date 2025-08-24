import { getLogs } from '$lib/api';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ fetch }) => {
	return {
		logs: await getLogs(fetch)
	};
};
