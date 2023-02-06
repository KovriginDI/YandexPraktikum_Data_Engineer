# Проект 1
Опишите здесь поэтапно ход решения задачи. Вы можете ориентироваться на тот план выполнения проекта, который мы предлагаем в инструкции на платформе.

# Витрина RFM

## 1.1. Выясните требования к целевой витрине.

Постановка задачи выглядит достаточно абстрактно - постройте витрину. Первым делом вам необходимо выяснить у заказчика детали. Запросите недостающую информацию у заказчика в чате.

Зафиксируйте выясненные требования. Составьте документацию готовящейся витрины на основе заданных вами вопросов, добавив все необходимые детали.

-----------

https://github.com/KovriginDI/de-project-1/blob/main/requirements.md

## 1.2. Изучите структуру исходных данных.

Полключитесь к базе данных и изучите структуру таблиц.

Если появились вопросы по устройству источника, задайте их в чате.

Зафиксируйте, какие поля вы будете использовать для расчета витрины.

-----------

Поля:
- production.orders.order_ts - дата и время заказа
- production.orders.user_id - уникальный идентификатор клиента
- production.orders.cost - сумма заказа
- production.orders.status - статус заказа
- production.orderstatuses.id - идентификатор статуса заказа
- production.orderstatuses.key - обозначение статуса заказа
- production.users.id - уникальный идентификатор клиента
- production.users.name - логин клиента
- production.users.login - ФИО клиента