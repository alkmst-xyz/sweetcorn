<script lang="ts">
	import type { AriaAttributes } from 'svelte/elements';
	import type { NavItemType } from './types';

	let {
		item,
		ariaCurrent
	}: { item: NavItemType; ariaCurrent?: AriaAttributes['aria-current'] } =
		$props();
</script>

<li
	aria-current={ariaCurrent}
	class="decoration-2 underline-offset-4 aria-[current=page]:font-semibold aria-[current=page]:underline"
>
	<a href={item.path}>{item.title}</a>
</li>
