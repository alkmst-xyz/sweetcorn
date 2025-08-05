import { getLogs } from '$lib/api';
import type { PageLoad } from './$types';

export const load: PageLoad = ({ fetch }) => {
	return getLogs(fetch);
};
