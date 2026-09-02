interface ImportMetaEnv {
	readonly ENDPOINT: string;
	readonly MODULE: string;
	readonly APP: string;
	readonly VITE_ACCESS_KEY: string;
	readonly VITE_SECRET_KEY: string;
	// more env variables...
}

interface ImportMeta {
	readonly env: ImportMetaEnv;
}

declare module "*.jpg" {
	const value: string;
	export = value;
}

declare module "*.png" {
	const value: string;
	export = value;
}

declare module "*.svg" {
	const content: string;
	export default content;
}
