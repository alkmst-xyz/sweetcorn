const API_BASE_URL_DEV = 'http://localhost:13579/';
const API_BASE_URL_PROD = '/';

// Load Sweetcorn API base URL from vite environment.
function getBaseURL(): string {
	const baseURL = import.meta.env.VITE_API_BASE_URL;
	if (import.meta.env.DEV) {
		return baseURL || API_BASE_URL_DEV;
	}
	return baseURL || API_BASE_URL_PROD;
}

export const API_BASE = getBaseURL();
