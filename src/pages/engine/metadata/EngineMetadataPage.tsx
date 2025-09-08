import { ArrowCircleDown, Create, Refresh } from "@mui/icons-material";
import { observer } from "mobx-react-lite";
import { useCallback, useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import {
	Button,
	Chip,
	IconButton,
	Modal,
	Stack,
	styled,
	Table,
	TextField,
	Typography
} from "@semoss/ui";
import { Metamodel } from "@/components/metamodel";
import { Section } from "@/components/ui";
import { useEngine, usePixel, useRootStore } from "@/hooks";
import { SyncChangesModal } from "./SyncChangesModal";

const StyledPage = styled("div")(() => ({
	position: "relative",
	zIndex: "0",
}));

const StyledMetamodelContainer = styled("section")(({ theme }) => ({
	height: "55vh",
	width: "100%",
	borderWidth: "1px",
	borderStyle: "solid",
	borderRadius: theme.shape.borderRadius,
}));

const StyledTableContainer = styled(Table.Container)(() => ({
	height: "396px",
}));

export const EngineMetadataPage = observer(() => {
	const { active } = useEngine();
	const { monolithStore } = useRootStore();

	const [selectedNode, setSelectedNode] = useState(null);
	const [columnPage, setColumnPage] = useState<number>(0);
	const [columnVisibleRows, setColumnVisibleRows] = useState<number>(5);

	const [customNodes, setCustomNodes] = useState(null);
	const [customEdges, setCustomEdges] = useState(null);
	const [relationships, setRelationships] = useState(null);
	const [canSave, setCanSave] = useState(true);
	
	// State for edit description dialog
	const [editDescriptionDialog, setEditDescriptionDialog] = useState({
		open: false,
		columnId: "",
		columnName: "",
		tableName: "",
		currentDescription: "",
		newDescription: "",
	});

	const navigate = useNavigate();

	const getDatabaseMetamodel = usePixel<{
		dataTypes: Record<string, "INT" | "DOUBLE" | "STRING">;
		logicalNames: Record<string, string[]>;
		nodes: { propSet: string[]; conceptualName: string }[];
		edges: {
			sourceColumn?: string;
			targetColumn?: string;
			relation: string;
			source: string;
			target: string;
		}[];
		physicalTypes: Record<string, string>;
		positions: Record<
			string,
			{
				top: number;
				left: number;
			}
		>;
		descriptions: Record<string, string>;
		additionalDataTypes: Record<string, "INT" | "FLOAT" | "VARCHAR(2000)">;
	}>(
		`GetDatabaseMetamodel( database=["${active.id}"], options=["dataTypes","additionalDataTypes","logicalNames","descriptions","positions"]); `,
	);

	const reloadDescriptions = useCallback(
		async (node = selectedNode) => {
			if (getDatabaseMetamodel.status !== "SUCCESS" || !node) return;
	
			try {
				// Use GetTableDescriptions to get all descriptions for the table at once
				const tableName = node.data.name;
				const pixel = `GetTableDescriptions(database=["${active.id}"], concept="${tableName}")`;
				const response = await monolithStore.runQuery(pixel);
				const tableDescriptions = response.pixelReturn[0]?.output || {};
				
				// Update the descriptions with all the new data
				const updatedDescriptions = { 
					...getDatabaseMetamodel.data.descriptions,
					...tableDescriptions
				};
	
				getDatabaseMetamodel.update({
					...getDatabaseMetamodel.data,
					descriptions: updatedDescriptions,
				});
			} catch (error) {
				console.error("Failed to reload descriptions:", error);
			}
		},
		[getDatabaseMetamodel, monolithStore, active.id, selectedNode]
	);
	
	// get the data if a table is selected
	const getData = usePixel<{
		data: {
			values: (string | number | boolean)[][];
			headers: string[];
		};
		headerInfo: {
			dataType: string;
			additionalDataType: string;
			alias: string;
			header: string;
			type: string;
			derived: boolean;
		}[];
		numCollected: number;
	}>(
		selectedNode && selectedNode.data.properties.length > 0
			? `Database(database=["${
					active.id
				}"]) | Select(${selectedNode.data.properties
					.map((p) => p.id)
					.join(", ")}) | Collect(100);`
			: "",
		{
			data: {
				data: {
					values: [],
					headers: [],
				},
				headerInfo: [],
				numCollected: 0,
			},
		},
	);

	const defaultNodes = useMemo(() => {
		if (getDatabaseMetamodel.status !== "SUCCESS") return [];
		const { nodes = [], positions = {} } = getDatabaseMetamodel.data;
		return nodes.map((n) => ({
			id: n.conceptualName,
			type: "metamodel",
			data: {
				name: n.conceptualName.replace(/_/g, " "),
				properties: n.propSet.map((p) => ({
					id: `${n.conceptualName}__${p}`,
					name: p.replace(/_/g, " "),
					type: "",
				})),
			},
			position: positions[n.conceptualName]
				? {
						x: positions[n.conceptualName].left,
						y: positions[n.conceptualName].top,
					}
				: { x: 0, y: 0 },
		}));
	}, [getDatabaseMetamodel.status, getDatabaseMetamodel.data]);

	const defaultEdges = useMemo(() => {
		if (getDatabaseMetamodel.status !== "SUCCESS") return [];
		return getDatabaseMetamodel.data.edges.map((e) => ({
			id: e.relation,
			type: "floating",
			source: e.source,
			target: e.target,
		}));
	}, [getDatabaseMetamodel.status, getDatabaseMetamodel.data]);

	useEffect(() => {
		if (selectedNode) {
			reloadDescriptions();
		}
	}, [selectedNode]);

	const [showSyncModal, setShowSyncModal] = useState(false);
	const [tabledata, setTabledata] = useState<string[]>([]);
	const [viewdata, setViewdata] = useState<string[]>([]);

	const refreshData = () => {
		const pixel = `ExternalUpdateJdbcTablesAndViews(database=["${active.id}"]);`;
		monolithStore.runQuery(pixel).then((response) => {
			const output = response.pixelReturn[0].output;
			setTabledata(output.tables ?? []);
			setViewdata(output.views ?? []);
			setShowSyncModal(true);
		});
	};

	const handleSyncApply = (
		selectedTables: string[],
		selectedViews: string[],
	) => {
		const filters = JSON.stringify([...selectedTables, ...selectedViews]);
		const pixel = `ExternalUpdateJdbcSchema(database=["${active.id}"], filters=${filters});`;

		monolithStore.runQuery(pixel).then((response) => {
			const output = response.pixelReturn[0]?.output;

			if (!output) return;

			const newNodes = output.tables.map((table) => ({
				id: table.table,
				type: "metamodel",
				data: {
					name: table.table.replace(/_/g, " "),
					properties: table.columns.map((col, idx) => ({
						id: `${table.table}__${col}`,
						name: col.replace(/_/g, " "),
						type: table.type?.[idx] || "",
					})),
				},
				position: output.positions?.[table.table]
					? {
							x: output.positions[table.table].left,
							y: output.positions[table.table].top,
						}
					: { x: 0, y: 0 },
			}));

			const newEdges = (output.relationships || []).map((rel, i) => ({
				id: `${rel.fromTable}-${rel.toTable}-${i}`,
				type: "floating",
				source: rel.fromTable,
				target: rel.toTable,
			}));

			setRelationships(output.relationships);
			setCustomNodes(newNodes);
			setCustomEdges(newEdges);
			setShowSyncModal(false);
			setCanSave(!true);
		});
	};

	const columnRows = useMemo(() => {
		if (!selectedNode?.data?.properties?.length) return [];
		return selectedNode.data.properties.slice(
			columnPage * columnVisibleRows,
			(columnPage + 1) * columnVisibleRows,
		);
	}, [selectedNode, columnPage, columnVisibleRows]);

	const description = selectedNode?.id
		? (getDatabaseMetamodel.data?.descriptions?.[selectedNode.id] ?? "")
		: "";

	const logical = selectedNode?.id
		? (getDatabaseMetamodel.data?.logicalNames?.[selectedNode.id] ?? [])
		: [];

	const printMeta = () => {
		const pixel = `META|DatabaseMetadataToPdf(database=["${active.id}"]);`;
		monolithStore.runQuery(pixel).then((response) => {
			const output = response.pixelReturn[0].output;
			const insightId = response.insightId;
			monolithStore.download(insightId, output);
		});
	};

	/**
	 *
	 * @param data
	 * @desc Needs to be done at top level since this is very similar to other RDBMS dbs
	 */
	const saveDatabase = async (data) => {
		const tables = {};
		const owlPositions = {};

		data.nodes.forEach((node) => {
			const tableInfo = node.data;
			const cols = node.data.properties;
			const firstCol = cols[0].name.replace(/ /g, "_");

			if (!tables[tableInfo.name + "." + firstCol]) {
				const columns = [];

				cols.forEach((col) => {
					columns.push(col.name.replace(/ /g, "_"));
				});

				tables[tableInfo.name + "." + firstCol] = columns;
			}

			if (!owlPositions[node.id]) {
				owlPositions[node.id] = {
					top: node.position.y,
					left: node.position.x,
				};
			}
		});

		const pixel = `RdbmsExternalUpload(database=["${active.id}"], metamodel=[${JSON.stringify({ relationships: relationships, tables: tables })}], existing=[true]); META|SaveOwlPositions(database=["${active.id}"], positionMap=[${JSON.stringify(owlPositions)}]); META|SyncDatabaseWithLocalMaster(database=["${active.id}"])`;

		const resp = await monolithStore.runQuery(pixel);
		const output = resp.pixelReturn[0].output,
			operationType = resp.pixelReturn[0].operationType;

		if (operationType.indexOf("ERROR") > -1) {
			console.warn("RDBMSExternalUpload Reactor bug");
		} else {
			navigate(`/engine/database/${output.database_id}/metamodel`);
			return;
		}
	};

	const onSubmit = () => {
		const payloadObj = {
			metamodel: {
				relation: [
					// { fromTable: 'id', toTable: 'Drug', relName: 'id_Drug' },
				],
				nodeProp: {
					// tableName: [ // 'colname' EXCLUDING THE FIRST COLUMN
					// ],
				},
			},
			dataTypeMap: {
				// colName: type,
			},
			newHeaders: {}, // oldColName: newColName
			additionalDataTypes: {}, // colName: specificFormat
			descriptionMap: {}, // colName: description
			logicalNamesMap: {}, // colName/alias: logicalName
			position: [{}],
			nodes: customNodes,
		};

		// build metamodel relation for each table
		for (const edge of customEdges) {
			const relName = `${edge.source}_${edge.target}`;
			payloadObj.metamodel.relation.push({
				fromTable: edge.source,
				toTable: edge.target,
				relName: relName,
			});
		}

		// build metamodel node prop

		// build dataTypeMap for each table
		for (const node of customNodes) {
			for (const col of node.data.properties) {
				payloadObj.dataTypeMap[col.name] = col.type;
			}

			payloadObj.metamodel.nodeProp[node.data.name] = [];
		}

		saveDatabase(payloadObj);
	};

	const handleEditDescriptionOpen = async (columnId: string, columnName: string, tableName: string, currentDescription: string) => {
		const logicalNames = getDatabaseMetamodel.data?.logicalNames?.[editDescriptionDialog.columnId] || [];
		// Use the first logical name if available, otherwise fallback to processing the column name
		const columnNameForPixel = logicalNames.length > 0 
			? logicalNames[0] 
			: editDescriptionDialog.columnName.replace(/\s+/g, "_");
			console.log("columnNameForPixel:    ", logicalNames);
			console.log("tableName:", tableName);

		// Use GetOwlDescriptions to get the most up-to-date description
		const pixel = `GetOwlDescriptions(database=["${active.id}"], concept="${tableName}", column="${columnNameForPixel}")`;
		console.log("GetOwlDescriptions pixel:", pixel);

		try {
			const response = await monolithStore.runQuery(pixel);
			const description = response.pixelReturn[0]?.output?.[columnName] || currentDescription;
			
			setEditDescriptionDialog({
				open: true,
				columnId,
				columnName,
				tableName,
				currentDescription: description,
				newDescription: description,
			});
		} catch (error) {
			console.error("Failed to retrieve description:", error);
			// Fallback to current description if API call fails
			setEditDescriptionDialog({
				open: true,
				columnId,
				columnName,
				tableName,
				currentDescription,
				newDescription: currentDescription,
			});
		}
	};

	const handleEditDescriptionClose = () => {
		setEditDescriptionDialog({
			...editDescriptionDialog,
			open: false,
		});
	};

	const handleDescriptionChange = (e: React.ChangeEvent<HTMLInputElement>) => {
		setEditDescriptionDialog({
			...editDescriptionDialog,
			newDescription: e.target.value,
		});
	};

	const handlePredictDescription = async () => {

		const logicalNames = getDatabaseMetamodel.data?.logicalNames?.[editDescriptionDialog.columnId] || [];
		
		const columnNameForPixel = logicalNames.length > 0 
			? logicalNames[0] 
			: editDescriptionDialog.columnName.replace(/\s+/g, "_");
			
		const pixel = `PredictOwlDescription(database=["${active.id}"], concept="${editDescriptionDialog.tableName}", column="${columnNameForPixel}")`;
		
		try {
			const response = await monolithStore.runQuery(pixel);
			const predictedDescription = response.pixelReturn[0]?.output?.[0] || "";
			
			setEditDescriptionDialog({
				...editDescriptionDialog,
				newDescription: predictedDescription,
			});
		} catch (error) {
			console.error("Failed to predict description:", error);
		}
	};

	const handleSaveDescription = async () => {
		// Use logical names instead of column name
		// Get the logical name from the metamodel data
		const logicalNames = getDatabaseMetamodel.data?.logicalNames?.[editDescriptionDialog.columnId] || [];
		// Use the first logical name if available, otherwise fallback to processing the column name
		const columnNameForPixel = logicalNames.length > 0 
			? logicalNames[0] 
			: editDescriptionDialog.columnName.replace(/\s+/g, "_");
			
		const pixel = `EditOwlDescription(database=["${active.id}"], concept="${editDescriptionDialog.tableName}", column="${columnNameForPixel}", description="${editDescriptionDialog.newDescription}")`;
		
		try {
			await monolithStore.runQuery(pixel);
			
			// Check if the table being updated is the one displayed in the data section (depends on getData)
			const isDataSectionTable = selectedNode && selectedNode.data.name === editDescriptionDialog.tableName;
			
			// Create the descriptions object for updating
			const descriptions: Record<string, string> = {};
			
			// Fix mapping: use the column name instead of numeric key
			if (isDataSectionTable) {
				// Use GetOwlDescriptions to get all descriptions for the table
				const logicalNames = getDatabaseMetamodel.data?.logicalNames?.[editDescriptionDialog.columnId] || [];
				// Use the first logical name if available, otherwise fallback to processing the column name
				const columnNameForPixel = logicalNames.length > 0 
					? logicalNames[0] 
					: editDescriptionDialog.columnName.replace(/\s+/g, "_");

				// Use GetOwlDescriptions to get the most up-to-date description
				const descriptionsPixel = `GetOwlDescriptions(database=["${active.id}"], concept="${editDescriptionDialog.tableName}", column="${columnNameForPixel}")`;
				const response = await monolithStore.runQuery(descriptionsPixel);

				// response.pixelReturn[0]?.output is giving you something like { "0": "mmmm" }
				const rawDescriptions = response.pixelReturn[0]?.output || {};
				console.log("rawDescriptions: ", rawDescriptions);
				console.log("response: ", response);

				// Fix mapping: use the column name instead of numeric key
				// If the API returns { "0": "someText" }, it should be saved as { [tableName__columnName]: "someText" }
				// If multiple valid keys exist (like employees__email, employees__hire_date…), merge them normally
				if (rawDescriptions["0"] && Object.keys(rawDescriptions).length === 1) {
					// Special case: only "0" key exists, map it to the tableName__columnName format
					const tableNameColumnName = `${editDescriptionDialog.tableName}__${columnNameForPixel}`;
					descriptions[tableNameColumnName] = rawDescriptions["0"];
				} else {
					// Normal case: multiple keys or keys are actual column names, merge them normally
					Object.keys(rawDescriptions).forEach(key => {
						// Skip the "0" key if there are other keys present
						if (key !== "0") {
							// If key already has tableName__ format, use as is
							// Otherwise, construct tableName__columnName format
							if (key.includes("__")) {
								descriptions[key] = rawDescriptions[key];
							} else {
								const tableNameColumnName = `${editDescriptionDialog.tableName}__${key}`;
								descriptions[tableNameColumnName] = rawDescriptions[key];
							}
						}
					});
				}
			} else {
				// For tables not displayed in the data section, just create the description entry
				const logicalNames = getDatabaseMetamodel.data?.logicalNames?.[editDescriptionDialog.columnId] || [];
				const columnNameForPixel = logicalNames.length > 0 
					? logicalNames[0] 
					: editDescriptionDialog.columnName.replace(/\s+/g, "_");
				const tableNameColumnName = `${editDescriptionDialog.tableName}__${columnNameForPixel}`;
				descriptions[tableNameColumnName] = editDescriptionDialog.newDescription;
			}
			
			console.log("fixed descriptions: ", descriptions);
			
			// Update the table data with the new descriptions
			const updatedData = { ...getDatabaseMetamodel.data };
			updatedData.descriptions = {
				...updatedData.descriptions,
				...descriptions,
			};

			// Update the getDatabaseMetamodel with new descriptions
			getDatabaseMetamodel.update(updatedData);
			console.log("data", getDatabaseMetamodel.data);
			console.log("updatedData: ", updatedData);

			// Also refresh the data table if it's the same table
			if (isDataSectionTable) {
				// Refresh the data table 
				getData.refresh();
				
				// Since refresh() doesn't return a Promise, we need to update descriptions in a different way
				// Schedule the description update after a brief delay to ensure refresh has completed
				setTimeout(() => {
					// Reapply the fixed descriptions after refresh to ensure they're preserved
					const refreshedData = { ...getDatabaseMetamodel.data };
					refreshedData.descriptions = {
						...refreshedData.descriptions,
						...descriptions,
					};
					getDatabaseMetamodel.update(refreshedData);
				}, 0);
			}

			handleEditDescriptionClose();
		} catch (error) {
			console.error("Failed to update description:", error);
		}
	};

	return (
		<StyledPage>
			<Section>
				<Section.Header
					actions={
						<Stack direction="row" spacing={2}>
							<Button variant="outlined" onClick={refreshData}>
								Refresh Data
							</Button>
							<Button
								startIcon={<ArrowCircleDown />}
								variant="outlined"
								onClick={printMeta}
								data-testid={"engine-metadata-print-btn"}
							>
								Print Metadata
							</Button>
							<Button
								disabled={canSave}
								variant="outlined"
								onClick={() => onSubmit()}
							>
								Save
							</Button>
						</Stack>
					}
				>
					Metamodel
				</Section.Header>
				<Stack spacing={2}>
					<StyledMetamodelContainer>
						<Metamodel
							nodes={customNodes ?? defaultNodes}
							edges={customEdges ?? defaultEdges}
							selectedNode={selectedNode}
							onSelectNode={setSelectedNode}
							isInteractive={true}
						/>
					</StyledMetamodelContainer>
				</Stack>
			</Section>

			{selectedNode && (
				<>
					<Section>
						<Section.Header>Description</Section.Header>
						<Typography variant="body2">{description}</Typography>
					</Section>

					<Section>
						<Section.Header>Logical Names</Section.Header>
						<Stack direction="row" spacing={1} flexWrap="wrap">
							{logical.map((name) => (
								<Chip
									key={name}
									label={name}
									color="primary"
									size="small"
								/>
							))}
						</Stack>
					</Section>

					<Section>
						<Section.Header>Columns</Section.Header>
						<StyledTableContainer>
							<Table stickyHeader>
								<Table.Head>
									<Table.Row>
										<Table.Cell>Edit</Table.Cell>
										<Table.Cell>Name</Table.Cell>
										<Table.Cell>Description</Table.Cell>
										<Table.Cell>Logical Names</Table.Cell>
									</Table.Row>
								</Table.Head>
								<Table.Body>
									{columnRows.map((property, idx) => {
										const desc =
											getDatabaseMetamodel.data
												?.descriptions?.[property.id] ||
											"";
										const logic =
											getDatabaseMetamodel.data
												?.logicalNames?.[property.id] ||
											[];
										return (
											<Table.Row
												key={`${property.id}--${idx}`}
											>
												<Table.Cell>
													<IconButton 
														onClick={() => handleEditDescriptionOpen(
															property.id,
															property.name,
															selectedNode.data.name,
															desc
														)}
													>
														<Create />
													</IconButton>
												</Table.Cell>
												<Table.Cell>
													{property.name}
												</Table.Cell>
												<Table.Cell>
													<Typography variant="caption">
														{desc}
													</Typography>
												</Table.Cell>
												<Table.Cell>
													<Stack
														direction="row"
														spacing={1}
														flexWrap="wrap"
													>
														{logic.map((ln) => (
															<Chip
																key={ln}
																label={ln}
																color="primary"
																size="small"
															/>
														))}
													</Stack>
												</Table.Cell>
											</Table.Row>
										);
									})}
								</Table.Body>
								<Table.Footer>
									<Table.Row>
										<Table.Pagination
											page={columnPage}
											count={
												selectedNode?.data?.properties
													?.length || 0
											}
											rowsPerPage={columnVisibleRows}
											rowsPerPageOptions={[7, 10, 25]}
											onPageChange={(e, v) =>
												setColumnPage(v)
											}
											onRowsPerPageChange={(e) =>
												setColumnVisibleRows(
													e.target
														.value as unknown as number,
												)
											}
										/>
									</Table.Row>
								</Table.Footer>
							</Table>
						</StyledTableContainer>
					</Section>
				</>
			)}

			{selectedNode && getData.status === "SUCCESS" && (
				<Section>
					<Section.Header>Data</Section.Header>
					<StyledTableContainer>
						<Table stickyHeader>
							<Table.Head>
								<Table.Row>
									{getData.data.data.headers.map((h) => (
										<Table.Cell key={h}>
											{h.replace(/_/g, " ")}
										</Table.Cell>
									))}
								</Table.Row>
							</Table.Head>
							<Table.Body>
								{getData.data.data.values.map((row, i) => (
									<Table.Row key={i}>
										{row.map((val, j) => (
											<Table.Cell key={j}>
												{val}
											</Table.Cell>
										))}
									</Table.Row>
								))}
							</Table.Body>
						</Table>
					</StyledTableContainer>
				</Section>
			)}

			<SyncChangesModal
				open={showSyncModal}
				onClose={() => setShowSyncModal(false)}
				onApply={handleSyncApply}
				tables={tabledata}
				views={viewdata}
			/>

			{/* Edit Description Dialog */}
			<Modal open={editDescriptionDialog.open} onClose={handleEditDescriptionClose}>
				<Modal.Title>
					<Stack direction="row" justifyContent="space-between" alignItems="center" width="100%">
						<span>Edit Column Description</span>
						<Button 
							variant="outlined" 
							size="small"
							onClick={handlePredictDescription}
						>
							Predict
						</Button>
					</Stack>
				</Modal.Title>
				<Modal.Content>
					<TextField
						label="Column Name"
						value={editDescriptionDialog.columnName}
						disabled
						fullWidth
						margin="normal"
					/>
					<TextField
						label="Description"
						value={editDescriptionDialog.newDescription}
						onChange={handleDescriptionChange}
						multiline
						rows={4}
						fullWidth
						margin="normal"
					/>
				</Modal.Content>
				<Modal.Actions>
					<Button onClick={handleEditDescriptionClose}>Cancel</Button>
					<Button onClick={handleSaveDescription} variant="contained">
						Save
					</Button>
				</Modal.Actions>
			</Modal>

		</StyledPage>
	);
});
