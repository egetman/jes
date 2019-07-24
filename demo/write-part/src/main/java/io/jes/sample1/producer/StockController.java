package io.jes.sample1.producer;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.jes.bus.CommandBus;
import io.jes.sample1.command.CreateItem;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static org.springframework.util.StringUtils.isEmpty;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/stock")
public class StockController {

    private final CommandBus bus;

    @PostMapping("/create")
    @SuppressWarnings("squid:S1452")
    public ResponseEntity<?> createItem(@RequestBody CreateItem command) {
        if (isEmpty(command.getItemName()) || command.getQuantity() <= 0) {
            log.warn("Bad request");
            return ResponseEntity.badRequest().body("Name can't be null & quantity must be > 0");
        }
        // sync dispatching with sync command bus
        bus.dispatch(command);
        return ResponseEntity.ok().build();
    }

}
