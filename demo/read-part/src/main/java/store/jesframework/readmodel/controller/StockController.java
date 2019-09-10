package store.jesframework.readmodel.controller;

import java.util.Collection;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import store.jesframework.readmodel.model.Item;
import store.jesframework.readmodel.repository.ItemRepository;
import lombok.RequiredArgsConstructor;

@RestController
@RequiredArgsConstructor
@RequestMapping("/stock")
public class StockController {

    private final ItemRepository itemRepository;

    @GetMapping("/list")
    public ResponseEntity<Collection<Item>> listItems() {
        return new ResponseEntity<>(itemRepository.findAll(), HttpStatus.OK);
    }

}
