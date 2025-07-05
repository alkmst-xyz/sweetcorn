<script lang="ts">
	import type { StatusResponse } from '$lib/api';
	import StatusIndicator from '$lib/components/StatusIndicator.svelte';
	import github from '$lib/images/github.svg';

	let { statusData }: { statusData: StatusResponse } = $props();
</script>

<footer class="flex-col space-y-2 border-t px-3 pb-6 pt-4">
	<StatusIndicator data={statusData} />

	<div class="flex w-full justify-between text-sm">
		<p>© 2025 Sweetcorn. Open source software.</p>

		<a
			href="https://github.com/alkmst-xyz/sweetcorn"
			class="flex items-center gap-1"
		>
			<div class="h-4 w-4">
				<img src={github} alt="GitHub" />
			</div>
			GitHub
		</a>
	</div>
</footer>
