/**
 * Navigation menu item.
 *
 * @property path - URL path.
 * @property title - Display name.
 * @property exact - Whether to match the `path` exactly. If `false`, the
 * item will match any URL that starts with `path`. Defaults to `true`.
 */
export type NavItemType = {
	path: string;
	title: string;
	exact?: boolean;
};
