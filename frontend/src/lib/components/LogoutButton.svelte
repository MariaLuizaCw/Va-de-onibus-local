<!-- Purpose: exibe o botão de logout com o mesmo layout/estilo reutilizável na tela principal. -->
<script lang="ts">
    import { createEventDispatcher } from 'svelte';

    const dispatch = createEventDispatcher<{ logout: void }>();

    export let disabled = false;
    export let label = 'Sair';

    function handleClick() {
        if (!disabled) {
            dispatch('logout');
        }
    }
</script>

<div class="auth-wrapper">
    <button type="button" class="secondary logout" on:click={handleClick} disabled={disabled}>
        {label}
    </button>
</div>

<style>
    .auth-wrapper {
        position: relative;
        z-index: 1;
    }

    .secondary {
        background: transparent;
        border: 1px solid rgba(248, 250, 252, 0.4);
        color: #f8fafc;
        padding: 0.8rem 1.25rem;
        border-radius: 999px;
        cursor: pointer;
        transition: transform 0.2s ease, border-color 0.2s ease;
    }

    .secondary:hover:not(:disabled) {
        transform: translateY(-1px);
        border-color: rgba(14, 165, 233, 0.8);
    }

    .secondary:disabled {
        opacity: 0.4;
        cursor: not-allowed;
    }

    .logout {
        font-weight: 600;
    }

    @media (max-width: 768px) {
        .auth-wrapper {
            order: 2;
        }
    }
</style>
