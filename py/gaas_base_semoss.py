from transformers import AutoTokenizer
from torch.utils.data import TensorDataset, random_split, DataLoader
from transformers import AutoTokenizer, AdamW


# base class for majority of the gaas classes
class Gaas_Base_Semoss:

    # set the data file, base model, input column, output column etc.
    def __init__(
        self,
        data_file=None,
        input_column=None,
        output_column=None,
        base_model="t5-base",
        model_output_dir=None,
        **kwargs,
    ):
        self.base_model = base_model
        self.input_column = input_column
        self.output_column = output_column
        self.data_file = data_file
        self.model_output_dir = model_output_dir

    def max_length(self):
        for sent in self.sentences:
            # Tokenize the text and add `[CLS]` and `[SEP]` tokens.
            input_ids = self.tokenizer.encode(sent, add_special_tokens=True)
            # Update the maximum sentence length.
            self.max_len = max(self.max_len, len(input_ids))
        return self.max_len

    def prep_data(self):
        assert self.data_file is not None
        df = pd.read_csv(self.data_file)
        if self.limit_ds is not None:
            df = df[: self.limit_ds]
        # for now only do 100
        # df = df[:100]
        print(f"File Statistics: Rows {df.shape[0]} and columns {df.shape[1]}")
        self.df = df
        self.sentences = df[self.input_column]
        self.labels = df[self.output_column]

        # df = df.sample(frac=1).reset_index(drop=True)
        # bins = int(np.floor(1 + np.log2(len(df))))
        # df.loc[:, "bins"] = pd.cut(df["target"], bins=bins, labels=False)
        # skf = StratifiedKFold(n_splits = 5)

        # for fold, (train_idx, val_idx) in enumerate(skf.split(X=df, y=df["bins"].values)):
        #  df.loc[val_idx, "skfold"] = fold
        # self.df = df

    def prep_model(self):
        # if self.model is None:
        print(self.base_model)
        from transformers import AutoModel

        self.model = AutoModel.from_pretrained(self.base_model)

    def prep_tokenizer(self):
        self.tokenizer = AutoTokenizer.from_pretrained(self.base_model)

    def load_hf_dataset(self, dataset_name=None):
        assert dataset_name is not None
        from datasets import load_dataset

        ret_ds = load_dataset(dataset_name)

    def load_model(self, model_path=None):
        assert model_path is not None
        import torch

        self.model = torch.load(model_path)

    def tokenize_text(self):
        self.prep_data()
        self.prep_tokenizer()

        input_ids = []
        attention_masks = []

        max_len = self.max_length() + 4
        for sent in self.sentences:
            encoded_dict = self.tokenizer.encode_plus(
                sent,
                add_special_tokens=True,
                max_length=max_len,
                pad_to_max_length=True,
                return_attention_mask=True,
                return_tensors="pt",
            )
            input_ids.append(encoded_dict["input_ids"])
            attention_masks.append(encoded_dict["attention_mask"])
        input_ids = torch.cat(input_ids, dim=0)
        attention_masks = torch.cat(attention_masks, dim=0)
        labels = torch.tensor(self.labels, dtype=torch.float)
        return TensorDataset(input_ids, attention_masks, labels)

    def prep_loader(self, batch_size=32, split_size=0.9, dataset=None):
        if dataset is None:
            dataset = self.tokenize_text()
        train_size = int(split_size * len(dataset))
        val_size = len(dataset) - train_size
        # Divide the dataset by randomly selecting samples.
        train_dataset, val_dataset = random_split(dataset, [train_size, val_size])
        self.train_loader = DataLoader(
            train_dataset, shuffle=True, batch_size=batch_size
        )
        self.val_loader = DataLoader(val_dataset, shuffle=False, batch_size=batch_size)
        return train_dataset, val_dataset

    def init_device(self):
        import torch

        if torch.cuda.is_available():
            self.device = torch.device("cuda")
            print("Using GPU.")
        else:
            print("No GPU available, using the CPU instead.")
            self.device = torch.device("cpu")

    def get_optimizer(self, lr=5e-5, eps=1e-8):
        return AdamW(
            self.model.parameters(),
            # lr=5e-5,
            lr=lr,
            eps=1e-8,
        )

    # manipulating the learning rate i.e. trying to decay
    def get_scheduler(self, epochs=5, optimizer=None):
        from transformers import get_linear_schedule_with_warmup

        print(f"{len(self.train_loader)} with epochs {epochs}")
        total_steps = (
            len(self.train_loader) * epochs
        )  # why does this not consider batch size
        self.scheduler = get_linear_schedule_with_warmup(
            optimizer, num_warmup_steps=0, num_training_steps=total_steps
        )

        return self.scheduler

    def predict_value(self, text):
        sent_tokens = self.tokenizer(
            text,
            max_length=250,
            padding="max_length",
            truncation=True,
            add_special_tokens=True,
            return_attention_mask=True,
            return_token_type_ids=True,
            return_tensors="pt",
        )
        inputs, masks, token_type_ids = (
            sent_tokens["input_ids"],
            sent_tokens["attention_mask"],
            sent_tokens["token_type_ids"],
        )
        prediction = self.model(inputs.to(self.device), masks.to(self.device))
        return prediction.cpu().detach().numpy()[0][0]
