# trainer.py

import torch
import torch.nn as nn
from torch.optim.lr_scheduler import StepLR

class Trainer:
    """
    Generic Trainer class for PyTorch models with callback support.
    """

    def __init__(
        self,
        model: nn.Module,
        train_data_loader,
        test_data_loader,
        device,
        epochs: int = 10,
        lr: float = 1e-3,
        step_size: int = 4,
        callbacks: list = None,
    ):
        self.model = model
        self.train_loader = train_data_loader
        self.test_loader = test_data_loader
        self.device = device
        self.epochs = epochs

        self.optimizer = torch.optim.Adam(model.parameters(), lr=lr)
        self.scheduler = StepLR(self.optimizer, step_size=step_size, gamma=0.5)
        self.criterion = nn.CrossEntropyLoss()

        self.callbacks = callbacks or []

        # Metrics
        self.epoch = 0
        self.val_loss = None
        self.val_acc = None
        self.average_train_loss = None
        self.average_train_accuracy = None
        self.learning_rate = None

    def _train_one_epoch(self):
        self.model.train()
        running_loss = 0.0
        correct = 0
        total = 0

        for images, labels in self.train_loader:
            images, labels = images.to(self.device), labels.to(self.device)

            self.optimizer.zero_grad()
            outputs = self.model(images)
            loss = self.criterion(outputs, labels)
            loss.backward()
            torch.nn.utils.clip_grad_norm_(self.model.parameters(), 1.0)
            self.optimizer.step()

            running_loss += loss.item() * images.size(0)

            # Calculate accuracy
            _, preds = torch.max(outputs, 1)
            correct += (preds == labels).sum().item()
            total += labels.size(0)

        avg_train_loss = running_loss / len(self.train_loader.dataset)
        self.average_train_loss = avg_train_loss

        avg_train_accuracy = correct / total * 100
        self.average_train_accuracy = avg_train_accuracy

        self.learning_rate = self.optimizer.param_groups[0]["lr"]

        print(f"[Epoch {self.epoch}/{self.epochs}] Avg Train Loss: {avg_train_loss:.4f} | Avg. Train Acc: {avg_train_accuracy:.4f}", end=" | ")

    def _validate(self):
        self.model.eval()
        correct, total = 0, 0
        val_loss = 0.0

        with torch.no_grad():
            for images, labels in self.test_loader:
                images, labels = images.to(self.device), labels.to(self.device)

                outputs = self.model(images)
                loss = self.criterion(outputs, labels)
                val_loss += loss.item() * images.size(0)

                _, preds = torch.max(outputs, 1)
                correct += (preds == labels).sum().item()
                total += labels.size(0)

        self.val_loss = val_loss / len(self.test_loader.dataset)
        self.val_acc = correct / total * 100

        print(f"Val Loss: {self.val_loss:.4f} | Val Acc: {self.val_acc:.2f}%")

    def fit(self):
        for epoch in range(1, self.epochs + 1):
            self.epoch = epoch
            self._train_one_epoch()
            self._validate()

            self.scheduler.step()

            # Trigger callbacks at the end of each epoch
            for callback in self.callbacks:
                callback.end_of_epoch_activity(self)