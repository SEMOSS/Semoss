import tseslint from 'typescript-eslint';
import eslintConfigPrettier from 'eslint-config-prettier';

/** @type {import('eslint').Linter.Config[]} */
export default [
    { files: ['**/*.{js,mjs,cjs,ts}'] },
    { ignores: ['portals/', '**/node_modules/*'] },
    ...tseslint.configs.recommended,
    eslintConfigPrettier,
];
