from weatherlab.live.live_orders import seed_tonights_live_orders


SEEDED_LIVE_ORDER_IDS = seed_tonights_live_orders()


if __name__ == '__main__':
    print(f'Seeded {len(SEEDED_LIVE_ORDER_IDS)} live orders.')
