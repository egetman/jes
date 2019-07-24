package io.jes.sample1.command;

import java.util.UUID;

import io.jes.Command;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@Getter
@ToString
@RequiredArgsConstructor
public class PlaceOrder implements Command {

    private final UUID itemUuid;
    private final long quantity;

}
