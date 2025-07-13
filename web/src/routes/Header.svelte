<script lang="ts">
	import { Nav, type NavItemType } from '$lib/components/Nav';

	const navItems: Array<NavItemType> = [
		{
			path: '/',
			title: 'Home'
		},
		{
			path: '/logs',
			title: 'Logs',
			exact: false
		},
		{
			path: '/about',
			title: 'About'
		}
	];
</script>

<header class="flex flex-col items-start space-y-2 px-4 py-4">
	<h1 class="text-lg font-semibold">Sweetcorn</h1>
	<Nav items={navItems} />
</header>
