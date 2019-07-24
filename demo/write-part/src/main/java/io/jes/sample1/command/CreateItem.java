package io.jes.sample1.command;

import io.jes.Command;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@Getter
@ToString
@RequiredArgsConstructor
public class CreateItem implements Command {

    private final String itemName;
    private final long quantity;

}
