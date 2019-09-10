package store.jesframework.writemodel.producer;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import store.jesframework.bus.CommandBus;
import store.jesframework.writemodel.command.PlaceOrder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/order")
public class OrderController {

    private final CommandBus bus;

    @PostMapping("/submit")
    @SuppressWarnings("squid:S1452")
    public ResponseEntity<?> placeOrder(@RequestBody PlaceOrder command) {
        if (command.getItemUuid() == null || command.getQuantity() <= 0) {
            log.warn("Bad request");
            return ResponseEntity.badRequest().body("Uuid can't be null & quantity must be > 0");
        }
        // sync dispatching with sync command bus
        try {
            bus.dispatch(command);
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        }
        return ResponseEntity.ok().build();
    }


}
