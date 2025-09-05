import webpack from 'webpack';
import { resolve } from 'path';
import HtmlWebpackPlugin from 'html-webpack-plugin';
import { config } from 'dotenv';
import WebpackBar from 'webpackbar';
import path from 'path';
import { fileURLToPath } from 'url';

// Load the .env variables
config({ path: './.env' });
const isProduction = process.env.NODE_ENV == 'production';

// If the mode is not production, load the .env.local variables too
if (!isProduction) {
    config({ path: './.env.local' });
}

// Store some information that will be re-used in variables
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ENDPOINT = process.env.ENDPOINT;
const MODULE = process.env.MODULE;

export default {
    entry: './src/main.tsx', // This is where the react app starts
    mode: isProduction ? 'production' : 'development',
    performance: {
        hints: false,
    },
    output: {
        path: resolve(__dirname, '../portals'), // This is where the compiled file will be placed
        filename: '[name].[contenthash].js',
        clean: true,
    },
    devServer: {
        host: 'localhost',
        hot: true,
        port: '3003',
        proxy: [
            {
                context: MODULE,
                target: ENDPOINT,
                changeOrigin: true,
                secure: false,
                preserveHeaderKeyCase: true,
            },
        ],
        historyApiFallback: true,
        client: {
            overlay: false,
        },
    },
    plugins: [
        new HtmlWebpackPlugin({
            favicon: './src/assets/favicon.svg',
            scriptLoading: 'module',
            template: './src/template.html',
        }),
        new webpack.ProvidePlugin({
            React: 'react',
            ReactDOM: 'react-dom',
        }),
        new webpack.DefinePlugin({
            'process.env': JSON.stringify(process.env),
        }),
        new WebpackBar(),
    ],
    module: {
        rules: [
            {
                test: /\.(eot|svg|ttf|woff|woff2|png|jpg|gif)$/i,
                type: 'asset',
            },
            {
                test: /\.tsx?$/,
                use: 'ts-loader',
            },
            {
                test: /\.css$/,
                use: ['css-loader'],
            },

            // Add your rules for custom modules here
            // Learn more about loaders from https://webpack.js.org/loaders/
        ],
    },
    resolve: {
        extensions: ['.tsx', '.ts', '.js'],
        alias: {
            '@': resolve(__dirname, './src'),
        },
    },
};
